package main

import "fmt"
import "math/rand"
import "os"
import "os/signal"
import "time"
import kafka "github.com/Shopify/sarama"
import geoip "github.com/oschwald/geoip2-golang"
import "net"

func producer(ints chan<- uint32, set []uint32, duration time.Duration) {
	fmt.Println("launching...")
	for {
		ints <- set[rand.Intn(len(set))]
		time.Sleep(duration)
	}
}

func intToIP(i uint32) string {
	a := (byte(i >> 24))
	b := (byte(i >> 16))
	c := (byte(i >> 8))
	d := (byte(i))
	str := fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
	return str
}

func consumer(src <-chan uint32, dst <-chan uint32, producer kafka.AsyncProducer) {
	topic := "network"

	db, err := geoip.Open("GeoLite2-City.mmdb")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	for {
		src_ip := <-src
		src_str := intToIP(src_ip)
		dst_ip := <-dst
		dst_str := intToIP(dst_ip)

		src_ip_parsed := net.ParseIP(src_str)
		dst_ip_parsed := net.ParseIP(dst_str)
		src_record, err := db.City(src_ip_parsed)
		if err != nil {
			fmt.Println(err)
		}
		dst_record, err := db.City(dst_ip_parsed)
		if err != nil {
			fmt.Println(err)
		}
        src_coords := fmt.Sprintf("%v,%v",src_record.Location.Latitude, src_record.Location.Longitude)
        dst_coords := fmt.Sprintf("%v,%v",dst_record.Location.Latitude, dst_record.Location.Longitude)
	
		str := src_str + "," + dst_str + "," + "\"" + src_coords + "\"" + "," + "\"" + dst_coords + "\"" + "," + time.Now().UTC().String()

		message := kafka.ProducerMessage{Topic: topic, Value: kafka.StringEncoder(str)}
		producer.Input() <- &message
	}
}

func main() {
	duration := 1 * time.Second
	src := make(chan uint32)
	dst := make(chan uint32)
	notify := make(chan os.Signal, 1)
	signal.Notify(notify, os.Interrupt, os.Kill)

	host := "127.0.0.1:9092"
	config := kafka.NewConfig()
	config.Producer.Return.Successes = true
	k_producer, err := kafka.NewAsyncProducer([]string{host}, config)
	if err != nil {
		panic(err)
	}

	srcs := []uint32{1625785863, 1625785864, 1625785865, 1625785866, 1625785867}
	bads := []uint32{1979570743, 3134782395}

    fmt.Println("src_ip,dst_ip,src_coord,dst_coord,received_at")

	go producer(src, srcs, duration)
	go producer(dst, bads, duration)
	go consumer(src, dst, k_producer)

	//you must read the producer.Successes() or else the producer will block.
	go func(producer kafka.AsyncProducer) {
		for {
			<-producer.Successes()
		}
	}(k_producer)

	s := <-notify
	fmt.Println("signal:", s)
	fmt.Println("done.")
}
