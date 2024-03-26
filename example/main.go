package main

import (
	`fmt`
	`time`
)

func main() {
	rf.Exchange()
	
}



var ex = rf.NewExchange(rf.Topic("main"), rf.MaxLife(time.Second*3))

func main() {

	runBg()
	runSub(4)
	// runSub(8)
	runTestSub()
	select {
	// case <-time.After(time.Second * 12):
	// 	ex.Close()
	case <-time.After(time.Second * 105):
		return
	}
}
func runTestSub() {
	go func() {
		for {
			time.Sleep(time.Second * 2)
			fmt.Println(ex.HasSubscribers())
		}

	}()
}

func runSub(subId int) {
	sub := ex.Subscriber()
	go func() {
		for {
			time.Sleep(time.Second * time.Duration(subId))
			fmt.Println("------------------")
			record, err := sub.Fetch()
			if nil != err {
				panic(err)
			}
			for _, r := range record {
				fmt.Printf("%d %s\n", subId, r.Body())
			}
			// sub.Commit()
			fmt.Println("------------------")
		}
	}()
	go func() {
		time.Sleep(time.Second * time.Duration(5))
		err := sub.Unsubscribe()
		if err != nil {
			return
		}
	}()

}

func runBg() {
	go func() {
		var i = 0
		for {
			i++
			time.Sleep(time.Second)
			err := ex.Publish([]byte(fmt.Sprintf("Message %d", i)))
			if err != nil {
				return
			}
		}
	}()
}
