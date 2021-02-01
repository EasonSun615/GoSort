package pipeline

import "sort"

func ArraySource(a ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		a := []int{}
		// Read into memory
		for v := range in {
			a = append(a, v)
		}
		// Sort
		sort.Ints(a)

		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		num1, ok1 := <-in1
		num2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && num1 < num2) {
				out <- num1
				num1, ok1 = <-in1
			} else {
				out <- num2
				num2, ok2 = <-in2
			}
		}
		close(out)
	}()

	return out
}
