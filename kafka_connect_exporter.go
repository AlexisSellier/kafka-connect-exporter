package main

import (
	"flag"
	"os"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listenAddress		= flag.String("web.listen-address", ":9121", "Address to listen on for web interface and telemetry.")
	metricPath		= flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	config             	= flag.String("config", "./config", "config file location.")
)


type connectorStatus struct {
	Name        string
	Tasks       []task
}

type task struct {
	State       string
}

type exporter struct {
	config      map[string]string
	runningTask *prometheus.Desc
	failingTask *prometheus.Desc
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.runningTask
	ch <- e.failingTask
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {
	for name, endpoint := range e.config {
		resp, err := http.Get("http://" + endpoint + "/connectors")
		if err != nil {
			log.Fatal("Cannot retrived connectors list: %v\n", err)
			return 
		}
		body, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		var connectors []string
		json.Unmarshal(body, &connectors)
		for _, connector := range connectors {
			status, error := http.Get("http://" + endpoint + "/connectors/" + connector + "/status")
			if error != nil {
				log.Fatal("Cannot retrieved connectors %s status: %v\n", connector, err)
				return 
			}
			defer status.Body.Close()
			var cstatus connectorStatus
			b, _ := ioutil.ReadAll(status.Body)
			json.Unmarshal(b, &cstatus)
			running := 0
			failing := 0
			for _, task := range cstatus.Tasks {
				if task.State == "RUNNING" {
					running++
				} else {
					failing++
				}
			}
			ch <- prometheus.MustNewConstMetric(e.runningTask,  prometheus.GaugeValue, float64(running), name, connector)
			ch <- prometheus.MustNewConstMetric(e.failingTask,  prometheus.GaugeValue, float64(failing), name, connector)
		}
	}
}

func newExporter(configfile string) *exporter {
	var e exporter
	file, err := ioutil.ReadFile(configfile)
	if err != nil {
		log.Fatal("File error: %v\n", err)
		os.Exit(1)
	}
	err = json.Unmarshal(file, &e.config)
	if err != nil {
		log.Fatal("Cannot parse confile file: %v\n", err)
		os.Exit(1)
	}
	e.runningTask = prometheus.NewDesc("kafka_connect_running_task", "Number of running task", []string{"cluster", "task"}, nil)
	e.failingTask = prometheus.NewDesc("kafka_connect_failing_task", "Number of failing task", []string{"cluster", "task"}, nil)
	return &e
}

func main() {
	flag.Parse()
	e := newExporter(*config)
	prometheus.MustRegister(e)
	prometheus.Unregister(prometheus.NewGoCollector())
	prometheus.Unregister(prometheus.NewProcessCollector(os.Getpid(), ""))
	http.Handle(*metricPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		       <head><title>Kafka connect exporter</title></head>
		       <body>
		       <h1>Kafka connect exporter</h1>
		       <p><a href='` + *metricPath + `'>Metrics</a></p>
		       </body>
		       </html>`))
	})
	log.Printf("providing metrics at %s%s", *listenAddress, *metricPath)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

