package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling

	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	RRSchedule(os.Stdout, "Round-robin", processes)

}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
		RemainingTime int64
		BurstTime     int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// sort processes by arrival time
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	// initialize the remaining time for all processes
	for i := range processes {
		processes[i].RemainingTime = processes[i].BurstDuration
	}

	// create a ready queue and add the first process
	readyQueue := make([]Process, 0, len(processes))
	readyQueue = append(readyQueue, processes[0])
	currentTime := processes[0].ArrivalTime

	for len(readyQueue) > 0 {
		// sort the ready queue by priority and remaining time
		sort.Slice(readyQueue, func(i, j int) bool {
			if readyQueue[i].Priority != readyQueue[j].Priority {
				return readyQueue[i].Priority < readyQueue[j].Priority
			}
			return readyQueue[i].RemainingTime < readyQueue[j].RemainingTime
		})

		current := readyQueue[0]

		// check if the current process has changed
		if len(gantt) == 0 || current.ProcessID != gantt[len(gantt)-1].PID {
			// calculate the waiting time for the current process
			waitingTime := currentTime - current.ArrivalTime

			// add the current process to the gantt chart
			gantt = append(gantt, TimeSlice{
				PID:   current.ProcessID,
				Start: currentTime,
				Stop:  currentTime + 1,
			})

			// update the schedule
			schedule[current.ProcessID-1] = []string{
				fmt.Sprint(current.ProcessID),
				fmt.Sprint(current.Priority),
				fmt.Sprint(current.BurstDuration),
				fmt.Sprint(current.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(current.BurstDuration + waitingTime),
				fmt.Sprint(current.BurstDuration + waitingTime + current.ArrivalTime),
			}

			// update the total wait and turnaround times
			totalWait += float64(waitingTime)
			totalTurnaround += float64(current.BurstDuration + waitingTime)

			// update the last completion time
			if current.ProcessID > processes[0].ProcessID {
				lastCompletion = float64(currentTime + 1)
			}
		}

		// update the remaining time for the current process
		current.RemainingTime--

		// update the current time and remaining times for other processes
		currentTime++
		for i := range processes {
			if processes[i].ArrivalTime <= currentTime && processes[i].RemainingTime > 0 && processes[i].ProcessID != current.ProcessID {
				processes[i].RemainingTime--
				if processes[i].RemainingTime < 0 {
					processes[i].RemainingTime = 0
				}
				readyQueue = append(readyQueue, processes[i])
			}
		}

		// check if there are any new processes that have arrived
		for i := range processes {
			if processes[i].ArrivalTime > currentTime {
				break
			}
			if processes[i].ArrivalTime <= currentTime && processes[i].RemainingTime > 0 && processes[i].ProcessID != current.ProcessID {
				readyQueue = append(readyQueue, processes[i])
			}
		}

		// check if the current process has completed
		if current.RemainingTime == 0 {
			// remove the current process from the ready queue
			for i := range readyQueue {
				if readyQueue[i].ProcessID == current.ProcessID {
					readyQueue = append(readyQueue[:i], readyQueue[i+1:]...)
					break
				}
			}
		}

		// calculate the average wait time, turnaround time and throughput
		count := float64(len(processes))
		aveWait := totalWait / count
		aveTurnaround := totalTurnaround / count
		aveThroughput := count / lastCompletion

		readyQueue = readyQueue[1:]

		outputTitle(w, title)
		outputGantt(w, gantt)
		outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

		break
	}
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// sort processes by arrival time
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	// initialize the remaining time for all processes
	for i := range processes {
		processes[i].RemainingTime = processes[i].BurstDuration
	}

	// create a ready queue and add the first process
	readyQueue := make([]Process, 0, len(processes))
	readyQueue = append(readyQueue, processes[0])
	currentTime := processes[0].ArrivalTime

	for len(readyQueue) > 0 {
		// sort the ready queue by priority and remaining time
		sort.Slice(readyQueue, func(i, j int) bool {
			if readyQueue[i].Priority != readyQueue[j].Priority {
				return readyQueue[i].Priority < readyQueue[j].Priority
			}
			return readyQueue[i].RemainingTime < readyQueue[j].RemainingTime
		})

		current := readyQueue[0]

		// check if the current process has changed
		if len(gantt) == 0 || current.ProcessID != gantt[len(gantt)-1].PID {
			// calculate the waiting time for the current process
			waitingTime := currentTime - current.ArrivalTime

			// add the current process to the gantt chart
			gantt = append(gantt, TimeSlice{
				PID:   current.ProcessID,
				Start: currentTime,
				Stop:  currentTime + 1,
			})

			// update the schedule
			schedule[current.ProcessID-1] = []string{
				fmt.Sprint(current.ProcessID),
				fmt.Sprint(current.Priority),
				fmt.Sprint(current.BurstDuration),
				fmt.Sprint(current.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(current.BurstDuration + waitingTime),
				fmt.Sprint(current.BurstDuration + waitingTime + current.ArrivalTime),
			}

			// update the total wait and turnaround times
			totalWait += float64(waitingTime)
			totalTurnaround += float64(current.BurstDuration + waitingTime)

			// update the last completion time
			if current.ProcessID > processes[0].ProcessID {
				lastCompletion = float64(currentTime + 1)
			}
		}

		// update the remaining time for the current process
		current.RemainingTime--

		// add the current process back to the ready queue if it still has remaining time
		if current.RemainingTime > 0 {
			readyQueue = append(readyQueue, current)
		}

		// update the current time and remaining times for other processes
		currentTime++
		for i := range processes {
			if processes[i].ArrivalTime <= currentTime && processes[i].RemainingTime > 0 && processes[i].ProcessID != current.ProcessID {
				processes[i].RemainingTime--
				if processes[i].RemainingTime < 0 {
					processes[i].RemainingTime = 0
				}
				readyQueue = append(readyQueue, processes[i])
			}
		}

		// check if there are any new processes that have arrived
		for i := range processes {
			if processes[i].ArrivalTime > currentTime {
				break
			}
			if processes[i].ArrivalTime <= currentTime && processes[i].RemainingTime > 0 && processes[i].ProcessID != current.ProcessID {
				readyQueue = append(readyQueue, processes[i])
			}
		}

		// check if the current process has completed
		if current.RemainingTime == 0 {
			// remove the current process from the ready queue
			for i := range readyQueue {
				if readyQueue[i].ProcessID == current.ProcessID {
					readyQueue = append(readyQueue[:i], readyQueue[i+1:]...)
					break
				}
			}

			// check if there are any new processes that have arrived
			for i := range processes {
				if processes[i].ArrivalTime > currentTime {
					break
				}
				if processes[i].ArrivalTime <= currentTime && processes[i].RemainingTime > 0 && processes[i].ProcessID != current.ProcessID {
					readyQueue = append(readyQueue, processes[i])
				}
			}

			// update the last completion time
			if current.ProcessID > processes[0].ProcessID {
				lastCompletion = float64(currentTime)
			}
		}

		// calculate the average wait time, turnaround time and throughput
		count := float64(len(processes))
		aveWait := totalWait / count
		aveTurnaround := totalTurnaround / count
		aveThroughput := count / lastCompletion

		outputTitle(w, title)
		outputGantt(w, gantt)
		outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

		break

	}

}

func RRSchedule(w io.Writer, title string, processes []Process) {
	numProcesses := len(processes)
	completedProcesses := 0
	currentTime := int64(0)
	totalWaitingTime := int64(0)
	totalTurnaroundTime := int64(0)
	lastTime := make([]int64, numProcesses)
	remainingTime := make([]int64, numProcesses)
	responseTime := make([]int64, numProcesses)
	queue := make([]int, 0)
	result := make([]string, 0)
	throughput := 0.0
	timeSlice := int64(1) // set timeSlice to 1 as default value

	for i := 0; i < numProcesses; i++ {
		remainingTime[i] = processes[i].BurstTime
		lastTime[i] = -1
	}

	var gantt []TimeSlice // initialize empty slice of TimeSlice

	for completedProcesses < numProcesses {
		for i := 0; i < numProcesses; i++ {
			if processes[i].ArrivalTime <= currentTime && remainingTime[i] > 0 {
				queue = append(queue, i)
			}
		}

		if len(queue) == 0 {
			currentTime++
			continue
		}

		process := queue[0]
		queue = queue[1:]

		if lastTime[process] == -1 {
			responseTime[process] = currentTime - processes[process].ArrivalTime
		}

		if remainingTime[process] <= timeSlice {
			currentTime += remainingTime[process]
			remainingTime[process] = 0
			completedProcesses++
			totalWaitingTime += currentTime - processes[process].BurstTime - processes[process].ArrivalTime
			totalTurnaroundTime += currentTime - processes[process].ArrivalTime
			result = append(result, fmt.Sprintf("%d\t\t%d\t\t%d", processes[process].ProcessID, currentTime-processes[process].BurstTime, currentTime))
			gantt = append(gantt, TimeSlice{PID: processes[process].ProcessID, Start: currentTime - processes[process].BurstTime, Stop: currentTime})
		} else {
			currentTime += timeSlice
			remainingTime[process] -= timeSlice
			result = append(result, fmt.Sprintf("%d\t\t%d\t\t%d", processes[process].ProcessID, currentTime-timeSlice, currentTime))
			gantt = append(gantt, TimeSlice{PID: processes[process].ProcessID, Start: currentTime - timeSlice, Stop: currentTime})
		}

		lastTime[process] = currentTime

		if remainingTime[process] > 0 {
			queue = append(queue, process)
		}
	}

	throughput = float64(numProcesses) / float64(currentTime)

	aveWait := float64(totalWaitingTime) / float64(numProcesses)
	aveTurnaround := float64(totalTurnaroundTime) / float64(numProcesses)
	aveThroughput := throughput

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, rows(processes), aveWait, aveTurnaround, aveThroughput)
}
func rows(processes []Process) [][]string {
	rows := make([][]string, len(processes))
	for i := range processes {
		rows[i] = []string{
			fmt.Sprintf("%d", processes[i].ProcessID),
			fmt.Sprintf("%d", processes[i].Priority),
			fmt.Sprintf("%d", processes[i].BurstTime),
			fmt.Sprintf("%d", processes[i].ArrivalTime),
			"",
			"",
			"",
		}
	}
	return rows
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
