package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/twinj/uuid"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app             = kingpin.New("examplco2", "An extended etcd demonstration")
	peerlist        = app.Flag("peers", "etcd peers").Default("http://127.0.0.1:4001,http://127.0.0.1:2379").OverrideDefaultFromEnvar("EX_PEERS").String()
	username        = app.Flag("user", "etcd User").OverrideDefaultFromEnvar("EX_USER").String()
	password        = app.Flag("pass", "etcd Password").OverrideDefaultFromEnvar("EX_PASS").String()
	cafile          = app.Flag("cacert", "CA Certificate").Required().String()
	config          = app.Command("config", "Change config data")
	configserver    = config.Arg("server", "Server name").Required().String()
	configvar       = config.Arg("var", "Config variable").Required().String()
	configval       = config.Arg("val", "Config value").Required().String()
	server          = app.Command("server", "Go into server mode and listen for changes")
	servername      = server.Arg("server", "Server name").Required().String()
	serverbeat      = app.Command("serverbeat", "Go into server mode and heartbeat in etcd")
	serverbeatname  = serverbeat.Arg("server", "Server name").Required().String()
	serverbeattime  = serverbeat.Flag("rate", "Time between beats").Default("60").Int()
	serverbeatcount = serverbeat.Flag("count", "Number of beats - 0 forever").Default("0").Int()
	serverwatch     = app.Command("serverwatch", "Watch for expiring servers")
)

var configbase = "/config/"
var runningbase = "/running/"

func main() {
	kingpin.Version("0.0.1")
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	peers := strings.Split(*peerlist, ",")

	caCert, err := ioutil.ReadFile(*cafile)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}

	cfg := client.Config{
		Endpoints:               peers,
		Transport:               transport,
		HeaderTimeoutPerRequest: time.Minute,
		Username:                *username,
		Password:                *password,
	}

	etcdclient, err := client.New(cfg)

	if err != nil {
		log.Fatal(err)
	}

	kapi := client.NewKeysAPI(etcdclient)

	switch command {
	case config.FullCommand():
		doConfig(kapi)
	case server.FullCommand():
		doServer(kapi)
	case serverbeat.FullCommand():
		doServerBeat(kapi)
	case serverwatch.FullCommand():
		doServerWatch(kapi)
	}
}

func doConfig(kapi client.KeysAPI) {
	var key = configbase + *configserver + "/" + *configvar

	resp, err := kapi.Set(context.TODO(), key, *configval, nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp.Action + " " + resp.Node.Key + " to " + resp.Node.Value)
}

func doServer(kapi client.KeysAPI) {
	var key = configbase + *servername

	var settings map[string]string
	settings = make(map[string]string)

	resp, err := kapi.Get(context.TODO(), key, &client.GetOptions{Recursive: true})
	if err != nil {
		log.Fatal(err)
	}

	for _, node := range resp.Node.Nodes {
		_, setting := path.Split(node.Key)
		settings[setting] = node.Value
	}

	fmt.Println(settings)

	watcher := kapi.Watcher(key, &client.WatcherOptions{Recursive: true})

	for true {
		resp, err := watcher.Next(context.TODO())

		if err != nil {
			if _, ok := err.(*client.ClusterError); ok {
				continue
			}
			log.Fatal(err)
		}

		switch resp.Action {
		case "set":
			_, setting := path.Split(resp.Node.Key)
			settings[setting] = resp.Node.Value
		case "delete", "expire":
			_, setting := path.Split(resp.Node.Key)
			delete(settings, setting)
		}

		fmt.Println(settings)
	}
}

func doServerBeat(kapi client.KeysAPI) {
	var key = runningbase + *serverbeatname

	myuuid := uuid.NewV4()
	uuidstring := myuuid.String()

	fmt.Println("Badum")
	_, err := kapi.Set(context.TODO(), key, uuidstring, &client.SetOptions{PrevExist: client.PrevNoExist, TTL: time.Second * 60})
	if err != nil {
		log.Fatal(err)
	}

	running := true
	counter := *serverbeatcount

	for running {
		time.Sleep(time.Second * time.Duration(*serverbeattime))
		fmt.Println("Badum")
		_, err := kapi.Set(context.TODO(), key, uuidstring, &client.SetOptions{PrevExist: client.PrevExist, TTL: time.Second * 60, PrevValue: uuidstring})
		if err != nil {
			log.Fatal(err)
		}
		if *serverbeatcount != 0 {
			counter = counter - 1
			if counter == 0 {
				running = false
			}
		}
	}

	_, err = kapi.Delete(context.TODO(), key, &client.DeleteOptions{PrevValue: uuidstring})
	if err != nil {
		log.Fatal(err)
	}
}

func doServerWatch(kapi client.KeysAPI) {

	watcher := kapi.Watcher(runningbase, &client.WatcherOptions{Recursive: true})

	for true {
		resp, err := watcher.Next(context.TODO())

		if err != nil {
			if _, ok := err.(*client.ClusterError); ok {
				continue
			}
			log.Fatal(err)
		}

		fmt.Println(resp.Node.Key + " " + resp.Node.Value)

		_, server := path.Split(resp.Node.Key)
		switch resp.Action {
		case "create":
			fmt.Println(server + " has started heart beat")
		case "compareAndSwap":
			fmt.Println(server + " heart beat")
		case "compareAndDelete":
			fmt.Println(server + " has shut down correctly")
		case "expire":
			fmt.Println("*** " + server + " has missed heartbeat")
		default:
			fmt.Println("Didn't handle " + resp.Action)
		}
	}

}
