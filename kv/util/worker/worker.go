package worker

import (
	"sync"
)

type TaskStop struct{}

type Task interface{}

type Worker struct {
	name     string
	sender   chan<- Task
	receiver <-chan Task
	closeCh  chan struct{}
	wg       *sync.WaitGroup
}

type TaskHandler interface {
	Handle(t Task)
}

type Starter interface {
	Start()
}

func (w *Worker) Start(handler TaskHandler) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		//【问题】为什么通过这种方式实现？还有不需要start的handler？
		// 或者所有的都实现start的方法，不就可以了？可以是空的方法；
		if s, ok := handler.(Starter); ok {
			s.Start()
		}
		for {

			Task := <-w.receiver

			//[question]这里的判断不是非常清楚的， Task出来是interface类型？
			//通过类型转换来判断是否退出，还是比较奇怪的；
			//anyway,是一种判断任务是否停止的方法；
			if _, ok := Task.(TaskStop); ok {
				return
			}
			// log.Printf("Task %v", Task)
			handler.Handle(Task)
		}
	}()
}

func (w *Worker) Sender() chan<- Task {
	return w.sender
}

func (w *Worker) Stop() {
	w.sender <- TaskStop{}
}

const defaultWorkerCapacity = 128

func NewWorker(name string, wg *sync.WaitGroup) *Worker {
	ch := make(chan Task, defaultWorkerCapacity)
	return &Worker{
		sender:   (chan<- Task)(ch),
		receiver: (<-chan Task)(ch),
		name:     name,
		wg:       wg,
	}
}
