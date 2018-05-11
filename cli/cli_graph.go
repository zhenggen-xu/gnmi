package cli

import (
	"context"
	"fmt"
	log "github.com/golang/glog"
	stats "github.com/zhenggen-xu/gnmi/cli/stats"
	"github.com/openconfig/gnmi/client"
	"encoding/json"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"io/ioutil"
	"sync"
	"time"
)

type groupRecord struct {
	grMap map[uint]*stats.Record //record for one session group
}

type cpuStat struct {
	CpuUsageAll cpuUtil   `json:"cpu_all"`
	CpuUsage    []cpuUtil `json:"cpus"`
}

// Cpu utilization rate
type cpuUtil struct {
	Id            string `json:"id"`
	CpuUtil_100ms uint64 `json:"100ms"`
	CpuUtil_1s    uint64 `json:"1s"`
	CpuUtil_5s    uint64 `json:"5s"`
	CpuUtil_1min  uint64 `json:"1min"`
	CpuUtil_5min  uint64 `json:"5min"`
}

func printCpuUtilization(r cpuStat) {
	b, err := json.MarshalIndent(&r, "", "  ")
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	log.V(1).Infof(string(b))

}
func runCpuStats(ctx context.Context, query client.Query, stop chan struct{}) {

	var cpuRecs []cpuStat
	var cpuLoadedRecs []cpuStat
	var cpuIdleRecs []cpuStat
	var err error

	query.NotificationHandler = func(n client.Notification) error {
		switch v := n.(type) {
		case client.Update:
			var rec cpuStat
			err = json.Unmarshal(v.Val.([]byte), &rec)
			if err != nil {
				log.V(1).Infof("Error: %v", v)
			}
			cpuRecs = append(cpuRecs, rec)
		case client.Delete:
		case client.Sync:
		case client.Error:
			log.V(1).Infof("Error: %v", v)
		}
		return nil
	}

	c, _ := stats.NewStatsClient(ctx, query.Destination())
	defer c.Close()
	log.V(1).Infof("runCpuStats loaded testing started")
	if err = c.Subscribe(ctx, query); err != nil {
		log.V(1).Infof("client had error while Subscribe: %v", err)
		return
	}
	if err = c.RecvAll(); err != nil {
		if err != nil {
			log.V(1).Infof("client had error while Subscribe Recv: %v", err)
			return
		}

	}
	for {
		if err = c.Poll(); err != nil {
			log.V(1).Infof("client.Poll(): %v", err)
			return
		}
		if err = c.RecvAll(); err != nil {
			if err != nil {
				log.V(1).Infof("client had error while Poll Recv: %v", err)
				return
			}
		}
		select {
		default:
			// default internal to be 1 second
			time.Sleep(time.Second)
		case _, more := <-stop:
			if more {
				log.V(1).Infof("runCpuStats number of record collected %v", len(cpuRecs))
				printCpuUtilization(cpuRecs[len(cpuRecs)-1])
				cpuLoadedRecs = cpuRecs
				log.V(1).Infof("runCpuStats idle testing started")
			} else {
				cpuIdleRecs = cpuRecs[len(cpuLoadedRecs):]
				log.V(1).Infof("runCpuStats number of record collected %v", len(cpuIdleRecs))
				printCpuUtilization(cpuRecs[len(cpuRecs)-1])
				plotCpuGraph("CPU utilization", "time (second)", "percents", cpuLoadedRecs, cpuIdleRecs)
				log.V(1).Infof("runCpuStats routine done")
				return
			}

		}

	}
}

// displayGraphResults collect request/response data and display them graphically
// Assuming poll request
func displayGraphResults(ctx context.Context, query client.Query, cfg *Config) error {
	// For each concurrent "session number" group
	var groupRecordMap = make(map[uint]groupRecord)

	maxS := cfg.ConcurrentMax
	if maxS < cfg.Concurrent {
		maxS = cfg.Concurrent
	}
	//var step uint = 1

	var grps []uint
	for grp := cfg.Concurrent; grp <= maxS; {
		grps = append(grps, grp)

		grp *= 2
		if grp > maxS && grp != 2*maxS {
			grp = maxS
		}
	}

	query.NotificationHandler = func(n client.Notification) error {
		switch v := n.(type) {
		case client.Update:
		case client.Delete:
		case client.Sync:
		case client.Error:
			cfg.Display([]byte(fmt.Sprintf("Error: %v", v)))
		}
		return nil
	}

	cpuQuery := query
	//cpuQuery.PollingInterval = 100 * time.Millisecond
	cpuQuery.Queries = []client.Path{{"platform", "cpu"}}
	cpuQuery.Target = "OTHERS"
	stopCh := make(chan struct{}, 1)
	go runCpuStats(ctx, cpuQuery, stopCh)
	ts := time.Now()

	for _, grp := range grps {
		grMap := make(map[uint]*stats.Record)
		groupRecordMap[grp] = groupRecord{grMap}

		cfg.Display([]byte(fmt.Sprintf("%v sessionGrp %v started ", time.Now().String(), grp)))
		var gMu sync.Mutex
		var w sync.WaitGroup
		for sessionNo := uint(1); sessionNo <= grp; sessionNo++ {
			// cfg.Display([]byte(fmt.Sprintf("%v session %v ", time.Now().String(), sessionNo)))
			w.Add(1)
			go func(grp uint, sessionNo uint) {
				//sessionCtx, cancel := context.WithCancel(context.Background())
				//defer cancel()
				// cfg.Display([]byte(fmt.Sprintf("start grp:sessionNo %v:%v , grMap : %v", grp, sessionNo, grMap)))

				defer w.Done()
				startTs := time.Now()
				c, _ := stats.NewStatsClient(ctx, query.Destination())
				defer c.Close()

				if err := c.Subscribe(ctx, query); err != nil {
					cfg.Display([]byte(fmt.Sprintf("client had error while Subscribe: %v", err)))
					return
				}
				if err := c.RecvAll(); err != nil {
					if err != nil {
						cfg.Display([]byte(fmt.Sprintf("client had error while Subscribe Recv: %v", err)))
						return
					}

				}
				ts_poll := time.Now()
				for count := cfg.Count; count > 0; count-- {
					log.V(2).Infof("count %v, actual working time %v", count, time.Since(ts_poll))
					time.Sleep(cfg.PollingInterval - time.Since(ts_poll))
					ts_poll = time.Now()
					if err := c.Poll(); err != nil {
						cfg.Display([]byte(fmt.Sprintf("client.Poll(): %v", err)))
						return
					}
					if err := c.RecvAll(); err != nil {
						if err != nil {
							cfg.Display([]byte(fmt.Sprintf("client had error while Poll Recv: %v", err)))
							return
						}
					}
				}
				if sessionNo == 1 {
					log.V(1).Infof("time taken for session 1 is %v with polling interval %v", time.Since(startTs), cfg.PollingInterval)
				}

				gMu.Lock()
				grMap[sessionNo] = &c.Rd
				gMu.Unlock()
			}(grp, sessionNo)
		}
		w.Wait()
		cfg.Display([]byte(fmt.Sprintf("%v sessionGrp %v done cfg.Count %v\n", time.Now().String(), grp, cfg.Count)))
	}
	time.Sleep(time.Millisecond * 100)
	stopCh <- struct{}{}
	time.Sleep(time.Since(ts))
	close(stopCh)
	time.Sleep(time.Second)

	for _, grp := range grps {
		grd := groupRecordMap[uint(grp)]
		cfg.Display([]byte(fmt.Sprintf("\t len(grd): %v, grp %v\n", len(grd.grMap), grp)))

		for sessionNo := uint(1); sessionNo <= grp; sessionNo++ {

			Rd, ok := grd.grMap[sessionNo]
			if !ok {
				cfg.Display([]byte(fmt.Sprintf("sessionGrp:sessionNo (%v:%v) len is 0\n",
					grp, sessionNo)))
				continue
			}
			if len(Rd.Sts) != len(Rd.Rts) || len(Rd.Sts) == 0 {
				cfg.Display([]byte(fmt.Sprintf("sessionGrp:sessionNo (%v:%v)  garbage records: len(Rts):len(Sts) %d:%d\n",
					grp, sessionNo, len(Rd.Rts), len(Rd.Sts))))
				continue
			}
			var diff time.Duration
			var validCnt int64
			for i := 0; i < len(Rd.Sts); i++ {
				if Rd.Rts[i].After(Rd.Sts[i]) || Rd.Rts[i].Equal(Rd.Sts[i]) {
					continue
				}
				validCnt++
				diff += Rd.Sts[i].Sub(Rd.Rts[i])
			}
			ms := int64(diff / time.Millisecond)
			ms /= validCnt
			cfg.Display([]byte(fmt.Sprintf("sessionGrp:sessionNo (%v:%v) latency %v ms\n", grp, sessionNo, ms)))
		}
	}
	saveJsonFile(grps, groupRecordMap)
	//plotGraph("Session Latency", "Session No.", "ms", groupRecordMap)
	if len(grps) > 1 {
		plotGroupsGraph("Session Group Latency", "Session Number", "ms", grps, groupRecordMap)
	} else {
		if cfg.Concurrent > 1 {
			// latency fluctuation among sessions
		} else {
			// latency fluctuation for one session
			groupRecord := groupRecordMap[grps[0]]
			plotSessionGraph("Single Session latency", "time (second)", "Latency (ms)", groupRecord.grMap[1])
		}
	}
	return nil
}

// saveJsonFile saves the raw data to json file
func saveJsonFile(grps []uint, grpMap map[uint]groupRecord) error {

	// To renders the data to map[string]interface{} for JSON marshall.
	msiGroup := make(map[string]interface{})

	for _, grp := range grps { // For each session group
		grmap := grpMap[grp].grMap

		msiSession := make(map[string]interface{})
		for sessNo, rd := range grmap { // for each session
			msiPoll := make(map[string]interface{})
			for i := 0; i < len(rd.Sts); i++ {
				msiTs := map[string]interface{}{}
				// poll time stamp , may use strconf.FormatInt()
				msiTs["poll_ts"] = rd.Rts[i].UnixNano()
				msiTs["synced_ts"] = rd.Sts[i].UnixNano()
				msiPoll[fmt.Sprintf("poll_%v", i)] = msiTs
			}
			msiSession[fmt.Sprintf("session_%v", sessNo)] = msiPoll
		}
		msiGroup[fmt.Sprintf("group_%v", grp)] = msiSession
	}
	j, err := json.MarshalIndent(msiGroup, "", "  ")
	if err != nil {
		return fmt.Errorf("JSON marshalling error: %v", err)
	}
	err = ioutil.WriteFile("raw_data.json", j, 0644)
	fmt.Printf("Raw latency data saved to raw_data.json\n")
	return err
}

//data for one session group
func addSessionPollPoints(rd *stats.Record) plotter.XYs {
	pts := make(plotter.XYs, len(rd.Sts))

	start := rd.Sts[0]
	for idx := 0; idx < len(rd.Sts); idx++ {
		diff := rd.Sts[idx].Sub(rd.Rts[idx])
		ms := int64(diff / time.Millisecond)

		x := float64(rd.Rts[idx].Sub(start) / time.Second)
		y := float64(ms)
		pts[idx].X = x
		pts[idx].Y = y
	}
	return pts
}

func plotSessionGraph(title, x, y string, rd *stats.Record) error {
	p, err := plot.New()
	if err != nil {
		log.V(1).Infof("plot %v", err)
		return err
	}

	p.Title.Text = title
	p.X.Label.Text = x
	p.Y.Label.Text = y

	err = plotutil.AddLinePoints(p,
		"Poll Session", addSessionPollPoints(rd))
	if err != nil {
		log.V(1).Infof("plotutil.AddLinePoints %v", err)
		return err
	}
	// Save the plot to a PNG file.
	if err := p.Save(16*vg.Inch, 8*vg.Inch, "session_latency.png"); err != nil {
		log.V(1).Infof("save PNG %v", err)
		return err
	}
	return nil
}

//data for one session group
func addCpuUtilPoints(cpuRecs []cpuStat) plotter.XYs {
	pts := make(plotter.XYs, len(cpuRecs))
	for idx, stat := range cpuRecs {
		rate := int64(stat.CpuUsageAll.CpuUtil_1s)
		x := float64(idx)
		y := float64(rate)
		pts[idx].X = x
		pts[idx].Y = y
	}
	return pts
}

func plotCpuGraph(title, x, y string, cpuLoadRecs, cpuRecs []cpuStat) error {
	p, err := plot.New()
	if err != nil {
		log.V(1).Infof("plot %v", err)
		return err
	}

	p.Title.Text = title
	p.X.Label.Text = x
	p.Y.Label.Text = y

	err = plotutil.AddLinePoints(p,
		"cpuWithTelemetry", addCpuUtilPoints(cpuLoadRecs),
		"cpuWithoutTelemetry", addCpuUtilPoints(cpuRecs))
	if err != nil {
		log.V(1).Infof("plotutil.AddLinePoints %v", err)
		return err
	}
	// Save the plot to a PNG file.
	if err := p.Save(16*vg.Inch, 8*vg.Inch, "cpu_utilization.png"); err != nil {
		log.V(1).Infof("save PNG %v", err)
		return err
	}
	return nil
}

// Average latency for this session group
func getGrpLatency(grmap map[uint]*stats.Record) int64 {
	var diff time.Duration
	var sessionCnt int64

	for _, rd := range grmap {
		for i := 0; i < len(rd.Sts); i++ {
			diff += rd.Sts[i].Sub(rd.Rts[i])
			sessionCnt++
		}
	}
	ms := int64(diff / time.Millisecond)
	ms /= sessionCnt
	return ms
}

//data for one session group
func addGroupPoints(grps []uint, grpMap map[uint]groupRecord) plotter.XYs {
	pts := make(plotter.XYs, len(grps))
	for idx, grp := range grps {
		ms := getGrpLatency(grpMap[grp].grMap)
		x := float64(grp)
		y := float64(ms)
		pts[idx].X = x
		pts[idx].Y = y
	}
	return pts
}

func plotGroupsGraph(title, x, y string, grps []uint, grpMap map[uint]groupRecord) error {
	p, err := plot.New()
	if err != nil {
		log.V(1).Infof("plot %v", err)
		return err
	}

	p.Title.Text = title
	p.X.Label.Text = x
	p.Y.Label.Text = y

	err = plotutil.AddLinePoints(p,
		"Groups", addGroupPoints(grps, grpMap))
	if err != nil {
		log.V(1).Infof("plotutil.AddLinePoints %v", err)
		return err
	}
	// Save the plot to a PNG file.
	if err := p.Save(16*vg.Inch, 8*vg.Inch, "group_latency.png"); err != nil {
		log.V(1).Infof("save PNG %v", err)
		return err
	}
	return nil
}

/*
//data for one session group
func addPoints(grmap map[uint]*stats.Record) plotter.XYs {
	pts := make(plotter.XYs, len(grmap))
	for sessionNo, rd := range grmap {
		var diff time.Duration
		for i := 0; i < len(rd.Sts); i++ {
			diff += rd.Sts[i].Sub(rd.Rts[i])
		}
		ms := int64(diff / time.Millisecond)
		ms /= int64(len(rd.Sts))

		x := float64(sessionNo)
		y := float64(ms)
		pts[sessionNo-1].X = x
		pts[sessionNo-1].Y = y
	}
	return pts
}

// plotGraph:
// <1> if only one session group and one session, generate latency data for each poll
// <2> if only one session group but multiple session, data not so interesting.
// <3> if number of session group is large than 1, generate group latency update.
func plotGraph(title, x, y string, grpMap map[uint]groupRecord) error {
	p, err := plot.New()
	if err != nil {
		log.V(1).Infof("plot %v", err)
		return err
	}

	p.Title.Text = title
	p.X.Label.Text = x
	p.Y.Label.Text = y

	var styleSet int
	for grp, grec := range grpMap {
		name := "group" + strconv.FormatUint(uint64(grp), 10)

		linePointsData := addPoints(grec.grMap)

		lpLine, lpPoints, err := plotter.NewLinePoints(linePointsData)
		if err != nil {
			return err
		}
		lpLine.Color = plotutil.Color(styleSet)
		lpLine.Dashes = plotutil.Dashes(styleSet)
		lpPoints.Color = plotutil.Color(styleSet)
		lpPoints.Shape = plotutil.Shape(styleSet)
		styleSet++

		p.Add(lpLine, lpPoints)
		p.Legend.Add(name, lpLine, lpPoints)
	}
	// Save the plot to a PNG file.
	if err := p.Save(16*vg.Inch, 8*vg.Inch, "latency.png"); err != nil {
		log.V(1).Infof("save PNG %v", err)
		return err
	}
	return nil
}
*/
