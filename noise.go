package main
import "fmt"
import "math/rand"
import "os"
import "os/signal"
import "time"
import kafka "github.com/Shopify/sarama"


func producer(ints chan<- int32,duration time.Duration) {
  fmt.Println("launching...")
  fmt.Println("The time to sleep is: ",duration)
  for {
    rand_int := rand.Int31()
    fmt.Println("Writing to chan: ",rand_int)
    ints <- rand_int
    fmt.Println("Sleeping now...")
    time.Sleep(duration)
  }
}

func intToIP(i int32) string {
  a := (byte(i>>24))
  b := (byte(i>>16))
  c := (byte(i>>8))
  d := (byte(i))
  str := fmt.Sprintf("%d.%d.%d.%d",a,b,c,d)
  return str
}

func consumer(src <-chan int32,dst <- chan int32,producer kafka.AsyncProducer) {
  topic := "test"
  for {
    src_ip := <- src
    src_str := intToIP(src_ip)
    dst_ip := <- dst
    dst_str := intToIP(dst_ip)
    str := src_str + "," + dst_str
    message := kafka.ProducerMessage{Topic: topic, Value: kafka.StringEncoder(str)}
    producer.Input() <- &message
  }
}

func main() {
  duration := 1 * time.Second
  src := make(chan int32)
  dst := make(chan int32)
  notify := make(chan os.Signal,1)
  signal.Notify(notify,os.Interrupt,os.Kill)

  host := "127.0.0.1:9092"
  config := kafka.NewConfig();
  config.Producer.Return.Successes = true;
  k_producer, err := kafka.NewAsyncProducer([]string{host}, config)
  if err != nil {
      panic(err)
  }



  go producer(src,duration)
  go producer(dst,duration)
  go consumer(src,dst,k_producer)


  go func(producer kafka.AsyncProducer) {
    for {
      <- producer.Successes()
      fmt.Println("did succeed.")
    }
  }(k_producer)

  s := <- notify
  fmt.Println("signal:",s)
  fmt.Println("done.")
  
}  
