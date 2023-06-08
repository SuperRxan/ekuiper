package report

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/mqtt"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
)

var VinCode = ""

var StatusReporter *Reporter

func GetVinCode() error {
	args := "shell getprop | grep vin"
	cmd := exec.Command("adb", args)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		conf.Log.Errorf(`err:%v stdout:%s stderr:%s`, err, outb.String(), errb.String())
		return err
	}
	//[persist.vehicled.vin]: [DV_PV_TASK_ENABLE]
	result := outb.String()
	splitResult := strings.Split(result, ":")
	if len(splitResult) == 2 {
		secondPart := splitResult[1]
		secondPart = strings.TrimLeft(secondPart, "[")
		secondPart = strings.TrimRight(secondPart, "]")
		VinCode = secondPart
	} else {
		conf.Log.Errorf(`not get expect result: %s`, splitResult)
		return fmt.Errorf(`not get expect result: %s`, splitResult)
	}
	return nil
}

const (
	ActionSync    = "sync"
	ActionCreate  = "create"
	ActionEdit    = "edit"
	ActionDelete  = "delete"
	ActionStart   = "start"
	ActionStop    = "stop"
	ActionRestart = "restart"
)

const (
	TypeData   = "data"
	TypeRule   = "rule"
	TypeStream = "stream"
	TypeTable  = "table"
	TypeConfig = "config"
	TypePlugin = "stop"
)

type Status struct {
	Type      string      `json:"type"`
	Action    string      `json:"action"`
	Id        string      `json:"id"`
	Vin       string      `json:"vin"`
	State     int         `json:"state"`
	EventTime int64       `json:"event_time"`
	ErrorMsg  string      `json:"err_msg,omitempty"`
	Content   interface{} `json:"content,omitempty"`
}

func NewStatus() Status {
	return Status{
		Type:      "rule",
		Action:    "edit",
		Id:        "",
		Vin:       VinCode,
		State:     1,
		EventTime: time.Now().Unix(),
	}
}

type Reporter struct {
	mqttClient mqtt.MQTTSink
	dataCh     chan Status
	ctx        *context.DefaultContext
	broker     string
	topic      string
}

func NewReporter(broker, topic string) (*Reporter, error) {
	re := &Reporter{
		broker: broker,
		topic:  topic,
	}
	re.dataCh = make(chan Status, 10)

	cli := mqtt.MQTTSink{}
	config := map[string]interface{}{}
	config["server"] = broker
	config["topic"] = topic
	config["qos"] = 0
	config["retained"] = true

	err := cli.Configure(config)
	if err != nil {
		return nil, err
	}

	contextLogger := conf.Log.WithField("report", "status")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	tf, _ := transform.GenTransform("", "json", "", "", "", []string{})
	vCtx := context.WithValue(ctx, context.TransKey, tf)

	err = cli.Open(ctx)
	if err != nil {
		return nil, err
	}

	re.mqttClient = cli
	re.ctx = vCtx
	StatusReporter = re
	return re, nil
}

func (r *Reporter) Report(status Status) {
	r.dataCh <- status
}

func (r *Reporter) Run() {
	for {
		select {
		case data := <-r.dataCh:
			logger := r.ctx.GetLogger()
			err := r.mqttClient.Collect(r.ctx, data)
			if err != nil {
				logger.Errorf("send status to broker error, broker: %s, topic: %s, error: %v", r.broker, r.topic, err)
			} else {
				logger.Infof("send status to broker success, broker: %s, topic: %s, data: %v ", r.broker, r.topic, data)
			}
		}
	}
}
