package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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
		fmt.Println("Cannot read file", err)
		return
	}

	defer file.Close() // close file after checking err

	queryStartTime, err := time.Parse("200601-02T15:04:05.0000Z", finishTimeArg)
	if err != nil {
		fmt.Println("Could not parse the finish time", finishTimeArg)
		return
	}

	queryFinishTime, err := time.Parse("2006-01-02T15:04:05.0000Z", finishTimeArg)
	if err != nil {
		fmt.Println("Could not parse the finish time", finishTimeArg)
		return
	}

	filestat, err := file.Stat()
	if err != nil {
		fmt.Println("Could not get the file stat")
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
		fmt.Println("Could not read the last line with offset", offset, "and last line size", lastLineSize)
		return
	}

	logSlice := strings.SplitN(string(lastLine), ",", 2)
	logCreationTimeString := logSlice[0]

	lastLogCreationTime, err := time.Parse("2006-01-02T15:04:05.0000Z", logCreationTimeString)
	if err != nil {
		fmt.Println("Cannot parse time :", err)
	}

	if lastLogCreationTime.After(queryStartTime) && lastLogCreationTime.Before(queryFinishTime) {
		Process(file, queryStartTime, queryFinishTime)
	}

	fmt.Println("\nTime taken -", time.Since(s))

}

func Process(f *os.File, start time.Time, end time.Time) error {

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 250*1024)
		return lines
	}}

	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}

	r := bufio.NewReader(f)

	var wg sync.WaitGroup

	for {
		buf := linesPool.Get().([]byte)

		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}

		nextUntilNewline, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, nextUntilNewline...)
		}

		wg.Add(1)
		go func() {
			ProcessChunck(buf, &linesPool, &stringPool, start, end)
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}
