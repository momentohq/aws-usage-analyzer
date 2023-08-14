package util

import "fmt"

func MultiPrintln(i []string) {
	for _, s := range i {
		fmt.Println(s)
	}
	fmt.Println("")
}
