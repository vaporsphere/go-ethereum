package network

import (
  "fmt"
)

type PriorityQueues struct {
  queues []chan interface{}
  wakeup chan bool
  quit chan bool
}

func NewPriorityQueues(n int, l int) {
  var queues = make(chan interface{}, n)
  for i, _ := range queues {
    queues[i] = make(chan interface{}, l)
  }
  return &PriorityQueues{
    queues: queues,
    wakeup: make(chan int)
  }
}

func (self *PriorityQueues) Run(f func(interface{}) error) {
go func() {
  var priority int = Top
  var q chan interface{}
READ:
  for {
    q = self.queues[priority]
    select {
    case <-self.quit:
      return
    case msg <- q:
      p.Send(msg)
      priority = Top
    default:
      if priority < Low {
        priority--
        continue READ
      }
      priority = Top
      select {
      case <- self.quit:
        return
case <- self.wakeup:
      }
    }
    }
      }()
}
