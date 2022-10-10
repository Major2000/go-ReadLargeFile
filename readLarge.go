package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {

	s := time.Now()
	args := os.Args[1:]
	if len(args) != 6 {
		fmt.Println("Please give proper command line arguments")
		return
	}

	startTimeArg := args[1]
	finishTimeArg := args[3]
	fileName := args[5]

	file, err := os.Open(fileName)

	if err != nil {
		fmt.Println("cannot able to read file", err)
		return
	}

	defer file.Close() // close file after checking err

	queryStartTime, err := time.Parse("200601-02T15:04:05.0000Z", finishTimeArg)
	if err != nil {
		fmt.Println("could not parse the finish time", finishTimeArg)
		return
	}

	filestat, err := file.Stat()
	if err != nil {
		fmt.Println("could ot get the file stat")
		return
	}

	fileSize := filestat.Size()
	offset := fileSize - 1
	lastLineSize := 0

	for {
		b := make([]byte, 1)
		n, err := file.ReadAt(b, offset)
		if err != nil {
			fmt.Println("Error reading file", err)
			break
		}

		char := string(b[0])
		if char == "\n" {
			break
		}
		offset--
		lastLineSize += n
	}

	lastLine := make([]byte, lastLineSize)
	_, err = file.ReadAt(lastLine, offset+1)

	if err != nil {
		fmt.Println("could not read the last line with offset", offset, "and last line size", lastLineSize)
		return
	}

	logSlice := strings.SplitN(string(lastLine), ",", 2)
	logCreationTimeString := logSlice[0]

	lastLogCreationTime, err := time.Parse("2006-01-02T15:04:05.0000Z", logCreationTimeString)

}
