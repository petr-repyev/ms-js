package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Config settings
type Config struct {

	// How manu cores will be used
	NumCPU int `yaml:"numcpu"`

	// Debug mode
	Debug bool `yaml:"debug"`

	// Beanstalk(producer) queue settings
	Queue struct {

		// Connection string
		DSN string `yaml:"dsn"`

		// Incoming tube
		In string `yaml:"in"`

		// Outgoing tube
		Out string `yaml:"out"`
	} `yaml:"queue"`

	// Workers options
	Worker struct {

		// TTL
		TTL int `yaml:"ttl"`

		// Num goroutines
		Num int `yaml:"num"`

		Useragent string `yaml:"useragent"`

		// Timeout seconds
		Timeout time.Duration `yaml:"timeout"`

		// Use keepalive
		Keepalive bool `yaml:"keepalive"`

		// Max idle connections per host
		Perhost int `yaml:"perhost"`
	} `yaml:"worker"`

	// Storage (consumer) options
	Storage struct {

		// Num goroutines
		Num int `yaml:"num"`

		// Mongodb connection
		DSN string `yaml:"dsn"`

		// Database name
		DB string `yaml:"db"`

		// Connection name
		Name string `yaml:"name"`

		// Timeout
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"storage"`

	// Database (MySQL) settings
	Database struct {

		// Connection string
		DSN string `yaml:"dsn"`

		// Max open connnections
		Open int `yaml:"open"`

		// Max idle connections
		Idle int `yaml:"idle"`

		// Connection max TTL
		TTL time.Duration `yaml:"ttl"`
	} `yaml:"database"`
}

// NumWorkers thread safe counter
type NumWorkers struct {
	mu sync.Mutex
	v  int
}

// Init counter init value
func (c *NumWorkers) Init() {
	c.mu.Lock()
	c.v = 0
	c.mu.Unlock()
}

// Inc increments the counter
func (c *NumWorkers) Inc() {
	c.mu.Lock()
	c.v++
	c.mu.Unlock()
}

// Dec Decrements the counter
func (c *NumWorkers) Dec() {
	c.mu.Lock()
	c.v--
	c.mu.Unlock()
}

// Value returns the current value of the counter
func (c *NumWorkers) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

// Job at queue
type Job struct {
	ID         uint64
	DomainID   uint64
	DomainName string
	DomainTld  string
	RequestID  uint64
}

// JobDone result data
type JobDone struct {
	JobID        uint64
	RequestID    uint64
	JobSucceed   bool
	HTML         string
	JS           []string
	DomainID     uint64
	DomainName   string
	DomainTld    string
	DomainEndURL string
}

// JobMetaspy inself
type JobMetaspy struct {
	RequestID uint64
}

// Beans queue
type Beans struct {

	// Mutex
	mtx sync.Mutex

	// Beanstalk connection
	c *beanstalk.Conn

	// Incoming tube
	tubeIn *beanstalk.TubeSet

	// Outgoing tube
	tubeOut *beanstalk.Tube
}

// Close queue connection
func (b *Beans) Close() error {
	return b.c.Close()
}

// Reserve incoming job
func (b *Beans) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	b.mtx.Lock()
	idv, bodyv, errv := b.tubeIn.Reserve(timeout)
	b.mtx.Unlock()
	return idv, bodyv, errv
}

// Put job into outgoing queue
func (b *Beans) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	b.mtx.Lock()
	idv, errv := b.tubeOut.Put(body, pri, delay, ttr)
	b.mtx.Unlock()
	return idv, errv
}

// NW num workers counter
var NW NumWorkers

// HTTPError state flag
var HTTPError = uint8(2)

// HTTPOk state flag
var HTTPOk = uint8(1)

// LoadConfig returns a new decoded Config struct
func LoadConfig(configPath string) (*Config, error) {
	// Create config structure
	config := &Config{}

	// Open config file
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Init new YAML decode
	d := yaml.NewDecoder(file)

	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

// MySQLConnection new connection
func MySQLConnection(DSN string, ttl time.Duration, maxOpen int, maxIdle int) (*sql.DB, error) {

	db, err := sql.Open("mysql", DSN)

	if err != nil {
		panic(err)
	}

	// disabled for production
	// db.SetConnMaxLifetime(time.Second * ttl)

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	return db, nil
}

// MongoConnection new conection
func MongoConnection(DSN string) (*mongo.Client, error) {

	client, err := mongo.NewClient(options.Client().ApplyURI(DSN))

	if err != nil {
		panic(err)
	}

	return client, nil
}

// DisableMediaLoad reduce traffics
func DisableMediaLoad(ctx context.Context) func(event interface{}) {
	return func(event interface{}) {
		switch ev := event.(type) {
		case *fetch.EventRequestPaused:
			go func() {
				copyCtx := chromedp.FromContext(ctx)
				ctx := cdp.WithExecutor(ctx, copyCtx.Target)

				isImage := ev.ResourceType == network.ResourceTypeImage
				isMedia := ev.ResourceType == network.ResourceTypeMedia
				isFont := ev.ResourceType == network.ResourceTypeFont
				isCSS := ev.ResourceType == network.ResourceTypeStylesheet

				stopDownload := isImage || isMedia || isFont || isCSS

				if stopDownload {
					fetch.FailRequest(ev.RequestID, network.ErrorReasonBlockedByClient).Do(ctx)
				} else {
					fetch.ContinueRequest(ev.RequestID).Do(ctx)
				}
			}()
		}
	}
}

// getGID return goroutine id (debug only)
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func main() {

	// Logrus settings
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})

	log.Info("Loading config.yml")

	// Loading config
	config, err := LoadConfig("/config.yml")

	if err != nil {
		log.Panic(err)
	}

	// @todo: use YML settings (config)
	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	// Limit SPU usage
	runtime.GOMAXPROCS(config.NumCPU)

	// Num workers
	NW.Init()

	// Runtime context
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	// Worker results channel
	wOut := make(chan JobDone)

	// Beanstalkd. Storage queue
	log.Debug("Connecting to queue")

	queue, err := beanstalk.Dial("tcp", config.Queue.DSN)

	if err != nil {
		log.Panic(err)
	}

	defer queue.Close()

	beans := Beans{
		c:       queue,
		tubeIn:  beanstalk.NewTubeSet(queue, config.Queue.In),
		tubeOut: beanstalk.NewTube(queue, config.Queue.Out),
	}

	// Database
	log.Debug("Connecting to MySQL")

	db, _ := MySQLConnection(config.Database.DSN, config.Database.TTL,
		config.Database.Open, config.Database.Idle)

	defer db.Close()

	log.WithFields(log.Fields{"N": config.Storage.Num}).Info("Starting Srotage goroutine(s)")

	// Storage
	for n := 0; n < config.Storage.Num; n++ {
		g.Go(func() error {
			return Storage(ctx, db, &beans, config, wOut)
		})
	}

	log.WithFields(log.Fields{"N": config.Worker.Num}).Info("Starting Workers goroutine(s)")

	// Workers
	for n := 1; n <= config.Worker.Num; n++ {
		g.Go(func() error {
			return Worker(ctx, config, wOut)
		})
	}

	// @todo: each worker has N renders, js-worker restarted
	// using docker --restart. v2.0 should have each make chromedp
	// instance restarted after N renders w/o docker restart
	g.Go(func() error {
		for {

			select {

			// Gracefull shutdown
			case <-ctx.Done():
				return ctx.Err()

			// Hard work here ...
			default:

				time.Sleep(1 * time.Second)

				if NW.Value() < config.Worker.Num {

					log.WithFields(log.Fields{"NW": NW.Value()}).Info("Spawning new Worker goroutine")

					g.Go(func() error {
						return Worker(ctx, config, wOut)
					})
				}
			}
		}
	})

	// OS Signal trap
	go func() {

		// OS Exit signals channel
		shutdownChannel := make(chan os.Signal, 1)

		// OS signals subscribtion
		signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

		<-shutdownChannel

		log.Error("Got termination signal, terminating goroutines ...")

		// Finishing goroutines context
		cancel()
	}()

	if err := g.Wait(); err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("Goroutines error")
	}

	log.Info("Exit")
}

// Storage goroutine updating mysql request status, storing response to mongo,
// moving to next queue.
func Storage(ctx context.Context, db *sql.DB, beans *Beans, config *Config, results <-chan JobDone) error {

	var (
		jobdone JobDone
		opStart time.Time
		opDura  time.Duration
	)

	// Goroutine ID
	gid := getGID()

	wLog := log.WithFields(log.Fields{
		"event": "storage",
		"id":    gid,
	})

	wLog.Debug("Connecting to MongoDB")

	// @todo: check if mongo has thread safe connections pool
	// Mongo Database
	MongoClient, _ := MongoConnection(config.Storage.DSN)

	mgctx, _ := context.WithTimeout(context.Background(), config.Storage.Timeout*time.Second)

	err := MongoClient.Connect(mgctx)

	if err != nil {
		return fmt.Errorf("Storage mongo connect panic (%s)", err.Error())
	}

	collection := MongoClient.Database(config.Storage.DB).Collection(config.Storage.Name)

	defer MongoClient.Disconnect(mgctx)

	for {

		wLog.Debug("Main loop begin")

		// Be nice to CPU
		time.Sleep(10 * time.Millisecond)

		select {

		// Gracefull shutdown
		case <-ctx.Done():
			return ctx.Err()

		// Hard work here ...
		default:

			wLog.Debug("Looking for a job")

			opStart = time.Now()

			select {

			// Picking up a job
			case jobdone = <-results:
				break

			// Gracefull shutdown
			case <-ctx.Done():
				return ctx.Err()
			}

			opDura = time.Since(opStart)

			wLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("Got a job")

			jobLog := wLog.WithFields(log.Fields{
				"job":     jobdone.JobID,
				"request": jobdone.RequestID,
			})

			if !jobdone.JobSucceed {

				jobLog.Debug("Failed")

				// @todo: use proper err codes
				sql := `UPDATE request
				   		   SET status = ?, created = UNIX_TIMESTAMP(), err = 100
				 		 WHERE id = ?`

				opStart = time.Now()

				_, err := db.Exec(sql, HTTPError, jobdone.RequestID)

				opDura = time.Since(opStart)

				if err != nil {
					return fmt.Errorf("Storage MySQL exec panic (%s)", err.Error())
				}

				jobLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("Database updated")

				jobLog.Debug("Main loop end")

				continue
			}

			jobLog.Debug("Saving to MongoDB")

			opStart = time.Now()

			_, err := collection.InsertOne(context.TODO(), jobdone)

			opDura = time.Since(opStart)

			if err != nil {
				return fmt.Errorf("Storage mongo insert panic (%s)", err.Error())
			}

			jobLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("MongoDB updated")

			payload, err := json.Marshal(&JobMetaspy{
				RequestID: jobdone.RequestID})

			if err != nil {
				return fmt.Errorf("Storage json panic (%s)", err.Error())
			}

			jobLog.Debug("Moving to next queue")

			opStart = time.Now()

			_, err = beans.Put([]byte(payload), 1, 0, 30*time.Minute)

			opDura = time.Since(opStart)

			if err != nil {
				return fmt.Errorf("Storage beanstalk insert panic. quit (%s)", err.Error())
			}

			jobLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("MongoDB updated")
		}

		wLog.Debug("Main loop end")
	}
}

// Worker picks a job and tries to make http request using chromedp instance
func Worker(ctx context.Context, config *Config, storage chan<- JobDone) error {

	var (
		job     Job
		opStart time.Time
		opDura  time.Duration
	)

	// limit for jsenv + html
	// Mongo doc size limit = 16mb
	docMaxSize := 5 * 1024 * 1024

	// Goroutine ID
	gid := getGID()

	// Number of workers (+1)
	NW.Inc()

	wLog := log.WithFields(log.Fields{
		"event": "worker",
		"id":    gid,
	})

	wLog.Debug("Connecting to beanstalkd")

	// Beanstalkd queue
	MQ, err := beanstalk.Dial("tcp", config.Queue.DSN)

	if err != nil {
		return fmt.Errorf("Queue connection error (%s)", err.Error())
	}

	defer MQ.Close()

	tubeIn := beanstalk.NewTubeSet(MQ, config.Queue.In)

	wLog.Debug("Creating chrome instance")

	opStart = time.Now()

	// Headless browser
	chromeOptions := []chromedp.ExecAllocatorOption{
		chromedp.DisableGPU,
		chromedp.NoSandbox,
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-web-security", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("blink-settings", "imagesEnabled=false"),
		chromedp.Flag("disable-images", true),
		chromedp.Flag("hide-scrollbars", false),
		chromedp.Flag("mute-audio", false),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-plugins", true),
		chromedp.Flag("disable-remote-fonts", true),
		chromedp.Flag("disable-reading-from-canvas", true),
		chromedp.Flag("disable-remote-playback-api", true),
		chromedp.Flag("disable-voice-input", true),
		chromedp.Flag("disable-accelerated-2d-canvas", true),
		chromedp.Flag("incognito", true),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("start-maximized", true),
		chromedp.Flag("disable-software-rasterizer", true),

		// @todo: uncomment for docker usage
		chromedp.ExecPath("/headless-shell/headless-shell"),
		chromedp.UserAgent(`Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36`),
	}

	chromeOptions = append(chromedp.DefaultExecAllocatorOptions[:], chromeOptions...)

	optCtx, cancelA := chromedp.NewExecAllocator(context.Background(), chromeOptions...)

	defer cancelA()

	chromeCtx, cancelB := chromedp.NewContext(optCtx, chromedp.WithLogf(log.Printf))

	defer cancelB()

	chromedp.Run(chromeCtx, make([]chromedp.Action, 0, 1)...)

	opDura = time.Since(opStart)

	wLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("Chromedp has been started")

	for z := 1; z <= config.Worker.TTL; z++ {

		wLog.WithFields(log.Fields{"NW": NW.Value()}).Debug("Main loop begin")

		// Defaults
		job = Job{}

		// Be nice to CPU
		time.Sleep(10 * time.Millisecond)

		select {

		// Gracefull shutdown
		case <-ctx.Done():
			return ctx.Err()

		// Hard work here ...
		default:

			wLog.Debug("Looking for a job")

			opStart = time.Now()

			// Pick up a job (release time 3 times more largest http timeout)
			id, body, err := tubeIn.Reserve(config.Worker.Timeout * 3 * time.Second)

			opDura = time.Since(opStart)

			if err != nil {
				// @todo use sleep, make graceful continue
				return fmt.Errorf("Queue reserve panic (%s)", err.Error())
			}

			wLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("Got a job")

			// load data
			json.Unmarshal(body, &job)

			job.ID = id

			jobLog := wLog.WithFields(log.Fields{
				"job":     job.ID,
				"request": job.RequestID,
			})

			// Render
			jobLog.Debug("Render worker begin")

			opStart = time.Now()

			// New Request (timeout)
			timeoutCtx, cancelC := context.WithTimeout(chromeCtx, config.Worker.Timeout*time.Second)

			defer cancelC()

			// disable media
			chromedp.ListenTarget(timeoutCtx, DisableMediaLoad(timeoutCtx))

			// Result page HTML source code
			html := ""

			// List window.* js variables
			var jsenv []string

			// Success status
			succeed := false

			// Request URL
			URL := fmt.Sprintf("http://%s.%s", job.DomainName, job.DomainTld)

			// Final page URL got by JS
			JsEndURL := ""

			// Render process
			err = chromedp.Run(timeoutCtx,

				// General
				page.SetDownloadBehavior(page.SetDownloadBehaviorBehaviorDeny),
				fetch.Enable(),

				// URL
				chromedp.Navigate(URL),

				// Javascript enviroment
				chromedp.Evaluate(`Object.keys(window);`, &jsenv),

				// End URL
				chromedp.Evaluate(`window.location.href;`, &JsEndURL),

				// Full html output
				chromedp.OuterHTML("html", &html),

				// Delay to load iframes, etc..
				chromedp.Sleep(3*time.Second),
			)

			// Page loaded correctly
			if err == nil && JsEndURL != "" {

				succeed = true

				jobLog.Debugf("Got JsEndURL %s for URL: %s", JsEndURL, URL)

				// Start point
				startURLObj, err := url.Parse(URL)

				if err != nil {
					jobLog.Errorf("Cannot parse start URL `%s`", URL)
					panic(err)
				}

				startHost := strings.ToUpper(startURLObj.Host)

				// End point
				endURLObj, err := url.Parse(JsEndURL)

				if err != nil {
					jobLog.Errorf("Cannot parse end URL `%s`", URL)
					panic(err)
				}

				endHost := strings.ToUpper(endURLObj.Host)

				if endURLObj.Port() != "" {
					noPort := ":" + endURLObj.Port()
					endHost = strings.ReplaceAll(endHost, noPort, "")
				}

				if strings.HasPrefix(endHost, "WWW.") {
					endHost = endHost[4:]
				}

				// Page has been redirected
				if startHost != endHost {
					jobLog.Errorf("Got redirect from `%s` to `%s`", URL, JsEndURL)
					succeed = false
				}

				// Error rendering page
			} else {

				succeed = false

				jobLog.WithFields(log.Fields{
					"JsEndURL": JsEndURL,
					"error":    err.Error(),
				}).Error("Page render failed")

			}

			// HTML document max size check
			if len(html) > docMaxSize {
				jobLog.WithFields(log.Fields{
					"max": docMaxSize,
					"doc": len(html),
				}).Errorf("HTML length size exceeded")

				html = html[0:docMaxSize]
			}

			opDura = time.Since(opStart)

			jobdone := JobDone{
				JobID:        job.ID,
				DomainName:   job.DomainName,
				DomainID:     job.DomainID,
				DomainTld:    job.DomainTld,
				RequestID:    job.RequestID,
				DomainEndURL: JsEndURL,
				JobSucceed:   succeed, // @todo: add http code/redirect checks (using cdp event listener, @see tag v1.3)
				HTML:         html,
				JS:           jsenv}

			jobLog.WithFields(log.Fields{
				"ms":      opDura.Milliseconds(),
				"succeed": succeed,
			}).Debug("Render end")

			jobLog.Debug("Deleting form queue begin")

			opStart = time.Now()

			// Delete from incoming queue
			err = MQ.Delete(job.ID)

			opDura = time.Since(opStart)

			if err != nil {
				return fmt.Errorf("Storage beanstalk delete panic(%s, id=%d)", err.Error(), id)
			}

			jobLog.WithFields(log.Fields{"ms": opDura.Milliseconds()}).Debug("Deleting form queue end")

			select {
			case storage <- jobdone:
				break

			// Gracefull shutdown
			case <-ctx.Done():
				return ctx.Err()
			}

			jobLog.Debug("Render worker end")
		}

		wLog.Debug("Main loop end")
	}

	// Number of workers (-1)
	NW.Dec()

	wLog.Debug("Worker TTL end")

	return nil
}
