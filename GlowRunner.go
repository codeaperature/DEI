package main

import (
	"flag"
	"strings"
	"bytes"
	"fmt"
	"io/ioutil"
	"github.com/chrislusf/glow/flow"
	"math/rand"
	"time"
	"net/http"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"golang.org/x/net/html"
	"gopkg.in/xmlpath.v1"
	"log"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	rss "github.com/jteeuwen/go-pkg-rss"
	"github.com/lazyshot/go-hbase"

//	"strconv"
//	"io"
	"os"
	"sort"
	"sync"
	"bufio"
//	"runtime"

)
type WordSentence struct {
	Word       string
	LineNumber int
}

type URLTuple struct {
	gURL       string
	s3Name      string
	msWait 		int
}

type statusTuple struct {
	pass     int
	fail 		int
}
var (
	fileName = flag.String("file", "/etc/passwd", "name of a text file")
	f1       = flow.New()
	f2       = flow.New()
	f3		 = flow.New()
	fw	 = flow.New()
)
type Feed struct {
	url string
	status int
	itemCount int
	complete bool
	itemsComplete bool
	index int
}

type FeedItem struct {
	feedIndex int
	complete bool
	url string
}


var conf struct {
	skillsCSV string
	hbaseZkURL string

}


var guidList []string
var feeds []Feed
var startTime int64
var timeout int
var feedSpace int

var wg sync.WaitGroup
var mutex *sync.Mutex

var skillMap map[string]int

type sortedMap struct {
	m map[string]int
	s []string
}



func init() {
	f1.Source(func(out chan WordSentence) {
		bytes, err := ioutil.ReadFile(*fileName)
		if err != nil {
			println("Failed to read", *fileName)
			return
		}
		lines := strings.Split(string(bytes), "\n")
		for lineNumber, line := range lines {
			for _, word := range strings.Split(line, " ") {
				if word != "" {
					out <- WordSentence{word, lineNumber}
				}
			}
		}
	}, 3).Map(func(ws WordSentence) (string, int) {
		return ws.Word, ws.LineNumber
	}).GroupByKey().Map(func(word string, lineNumbers []int) {
		fmt.Printf("%s : %v\n", word, lineNumbers)
	})

	f2.TextFile(
		"/etc/passwd", 2,
	).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, " ") {
			ch <- token
		}
	}).Map(func(key string) (string, int) {
		return key, 1
	}).ReduceByKey(func(x int, y int) int {
		// println("reduce:", x+y)
		return x + y
	}).Map(func(key string, x int) {
		println(key, ":", x)
	})

	f3.TextFile(
		"/etc/passwd", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, ":") {
			ch <- token
			//			println("token:", token)
		}
	}).Map(func(key string) int {
		return 1
	}).Reduce(func(x int, y int) int {
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	})

}


func (sm *sortedMap) Len() int {
	return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
	return sm.m[sm.s[i]] > sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]int) []string {
	sm := new(sortedMap)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0
	for key, _ := range m {
		sm.s[i] = key
		i++
	}
	sort.Sort(sm)
	return sm.s
}

func grabFeed2(feed *Feed, feedChan chan bool) {

	startGrab := time.Now().Unix()
	startGrabSeconds := startGrab - startTime
	fmt.Println("Grabbing feed",feed.url," at" ,startGrabSeconds,"second mark")

	if feed.status == 0 {
		fmt.Println("Feed not yet read")
		feed.status = 1
		//startX := int(startGrabSeconds * 33);
		startY := feedSpace * (feed.index)
		fmt.Println(startY)
		wg.Add(1)
		rssFeed := rss.New(timeout, true, channelHandler, itemsHandler);

		if err := rssFeed.Fetch(feed.url, nil); err != nil {
			fmt.Fprintf(os.Stderr, "[e] %s: %s", feed.url, err)
			return
		} else {
			endSec := time.Now().Unix()
			endX :=( (endSec - startGrab) )
			//			if endX == 0 {
			//				endX = 1
			//			}
			fmt.Println("Read feed in",endX,"seconds")
			wg.Wait()

			//			endGrab := time.Now().Unix()
			//			endGrabSeconds := endGrab - startTime
			//			feedEndX := int(endGrabSeconds * 33);

			feedChan <- true
		}
	} else if feed.status == 1 {
		fmt.Println("Feed already in progress")
	}
}

func getFromURL(URL string) {
}

func channelHandler(feed *rss.Feed, newchannels []*rss.Channel) {

}

func itemsHandler(feed *rss.Feed, ch *rss.Channel, newitems []*rss.Item) {

	fmt.Println("Found ",len(newitems)," items in ",feed.Url)

	for i := range newitems {
		//		fmt.Printf("STRUCT: %T\n"+ *newitems[i])

//		fmt.Printf("\nITEM: %d\n", i)
		fmt.Println("Item %d, Guid: %s", i, *newitems[i].Guid)
		guidList = append(guidList, *newitems[i].Guid)
		fmt.Println("Title      : " + newitems[i].Title)
		fmt.Println("Author     : "+ newitems[i].Author.Name )
		mutex.Lock()

		for j := 0; j < len(newitems[i].Categories); j++ {
			if val, ok := skillMap[newitems[i].Categories[j].Text]; ok {
				skillMap[newitems[i].Categories[j].Text] = val + 1
			} else {
				skillMap[newitems[i].Categories[j].Text] = 1
			}
			fmt.Printf("  Categories[%d] : %s [running count = %d]\n", j, newitems[i].Categories[j].Text, skillMap[newitems[i].Categories[j].Text]) // +, ", "+ newitems[i].Categories[0].Domain )
		}

		mutex.Unlock()
//		fmt.Println("Description: "+ newitems[i].Description )
//		fmt.Println("Pub Date   : "+ newitems[i].PubDate)
//		fmt.Println("Updated    : "+ newitems[i].Updated )
		//		fmt.Printf("%+v\n", newitems[0].Extensions)
		//		fmt.Printf("%+v\n", newitems[0].Extensions["http://www.w3.org/2005/Atom"])
		//		fmt.Printf("%+v\n", newitems[0].Extensions["http://stackoverflow.com/jobs/"])
		//		fmt.Printf("%+v\n", newitems[0].Extensions["http://stackoverflow.com/jobs/"]["location"])
		//		fmt.Printf("%+v\n", newitems[0].Extensions["http://stackoverflow.com/jobs/"]["location"][0].Value)
//		fmt.Println("Location    : "+ newitems[0].Extensions["http://stackoverflow.com/jobs/"]["location"][0].Value)

	}
	wg.Done()
}



func getRSS2() {
	startTime = time.Now().Unix()

	feedChan := make(chan bool, len(feeds))
	for i := range feeds {
		feeds[i].status = 0
		go grabFeed2(&feeds[i], feedChan)
		<- feedChan
	}
}


func loadSkillMapFile(m map[string]int) {
	// open input file
	sfile, err := os.Open(conf.skillsCSV)
	if err != nil {
		log.Printf("loadSkillMap file open error: %s\n", err.Error())
	} else {
		// close sfile on exit and check for its returned error
		defer func() {
			if err := sfile.Close(); err != nil {
				log.Fatalf("loadSkillMap file close error: %s\n", err.Error())
			}
		}()

		scanner := bufio.NewScanner(sfile)
		for scanner.Scan() {
			//'scanner.Text()' represents the test case, do something with it
			fmt.Println(scanner.Text())
			items := strings.Split(scanner.Text(), ",")
			m[items[0]] = 0
		}
	}
}

func saveSkillMapFile(skMap map[string]int) {
	// open input file
	sfile, err := os.Create(conf.skillsCSV)
	if err != nil {
		log.Fatalf("saveSkillMap file create error: %s\n", err.Error())
	}
	// close sfile on exit and check for its returned error
	defer func() {
		if err := sfile.Close(); err != nil {
			log.Fatalf("saveSkillMap file close error: %s\n", err.Error())
		}
	}()

	keysSorted := sortedKeys(skMap)

	for val, key := range keysSorted {
		fmt.Printf("%d - %s => %d\n", val, key, skillMap[key])
		sfile.WriteString(fmt.Sprintf("%s,%v\n",key,skillMap[key]))
	}
}

func saveSkillsMapHBase(skMap map[string]int) {
	log.Println("\n\n*** SETUP CLIENT ***\n")
	client := hbase.NewClient([]string{conf.hbaseZkURL}, "/hbase-unsecure")


	keysSorted := sortedKeys(skMap)

	for val, key := range keysSorted {
		fmt.Printf("%d - %s => %d\n", val, key, skillMap[key])
		//           key
		put := hbase.CreateNewPut([]byte(key))
		//                 family, column ,    value
		put.AddStringValue("v1", "count", fmt.Sprintf("%v",skillMap[key]))
		//		log.Printf("put-v = %T, %+v\n", put, put)
		//                      table
		res, err := client.Put("skills", put)
		log.Printf("Put Done: res = %T, %v\n", res, err)
	}
	// in hbase shell	scan 'skills', {VERSIONS=>10}
}


func getHostConfig() {
	name, err := os.Hostname()
	if err != nil {
		fmt.Printf("Hostname lookup Error: %v\n", err)
		return
	}
	if name != "neon" && name != "ip-172-31-61-13" {
		name = "neon"
	}
	if name == "neon" {
		conf.skillsCSV = "/Users/stephan/Galvanize/MyCapstone/skills.csv"
		conf.hbaseZkURL = ""
	} else if name == "ip-172-31-61-13" {
		conf.skillsCSV = "/home/ec2-user/golang/data/skills.csv"
		conf.hbaseZkURL = "ip-172-31-61-13.ec2.internal:2181"
	}
}


func main() {
	flag.Parse()
//	panic("Just Quit")
	getHostConfig()
	//	runtime.GOMAXPROCS(2)
	timeout = 1000
	fmt.Println("Feeds")
	//http://careers.stackoverflow.com/jobs/feed?searchTerm=big+data&location=san+francisco&range=100&distanceUnits=Miles
	//	feeds = append(feeds, Feed{index: 0, url: "http://careers.stackoverflow.com/jobs/feed?searchTerm=big+data&location=san+francisco&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })

	feeds = append(feeds, Feed{index: 0, url: "http://careers.stackoverflow.com/jobs/feed?location=san+francisco%2c+ca&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	feeds = append(feeds, Feed{index: 1, url: "http://careers.stackoverflow.com/jobs/feed?location=new+york+city%2c+ny&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	feeds = append(feeds, Feed{index: 2, url: "http://careers.stackoverflow.com/jobs/feed?location=los+angeles%2c+ca&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	feeds = append(feeds, Feed{index: 3, url: "http://careers.stackoverflow.com/jobs/feed?location=boston%2c+ma&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	feeds = append(feeds, Feed{index: 4, url: "http://careers.stackoverflow.com/jobs/feed?location=seattle%2cwa&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	feeds = append(feeds, Feed{index: 5, url: "http://careers.stackoverflow.com/jobs/feed?location=austin%2ctx&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	feeds = append(feeds, Feed{index: 6, url: "http://careers.stackoverflow.com/jobs/feed?location=chicago%2cil&range=100&distanceUnits=Miles", status: 0, itemCount: 0, complete: false, itemsComplete: false })
	mutex = &sync.Mutex{}
	skillMap = make(map[string]int, 200)
	loadSkillMapFile(skillMap)
	fmt.Println("GetRSS")
	getRSS2()
	saveSkillMapFile(skillMap)
	if(conf.hbaseZkURL != "") { saveSkillsMapHBase(skillMap)}

	for i:= 0; i < len(guidList); i++ {
		fmt.Println(guidList[i])
	}

//	guidList := make([]string, 4)
//	guidList[0] = "http://careers.stackoverflow.com/jobs/103310/senior-software-engineer-american-society-of-clinical"
//	guidList[1] = "http://careers.stackoverflow.com/jobs/94152/senior-software-engineer-platform-flixster"
//	guidList[2] = "http://careers.stackoverflow.com/jobs/103328/senior-full-stack-engineer-data-science-adroll"
//	guidList[3] = "http://careers.stackoverflow.com/jobs/104086/enterprise-architect-new-relic"
//	fmt.Printf("%v\n", s)

	// map random times & make s3names
	fw.Slice(guidList).Map(func(sURL string) URLTuple {
		fmt.Printf("Map1: %v\n", sURL)
		fName := "jobs_sof/" + strings.Replace(strings.TrimPrefix(sURL, "http://careers.stackoverflow.com/jobs/"), "/","_", -1)
		ms := rand.Intn(3000)
		return URLTuple{sURL, fName, ms}
//	Filter already-acquired URLs
	}).Filter(func(uTuple URLTuple) bool {
		// is file already stored in S3?
		//fmt.Printf("Filter:%s, %v\n", uTuple.s3Name, uTuple)
		svcS3 := s3.New(session.New(&aws.Config{Region: aws.String("us-east-1")}))
		var params *s3.HeadObjectInput

		params = &s3.HeadObjectInput{
			Bucket: aws.String("opps"), // Required
			Key: aws.String(uTuple.s3Name), // Required
		}
		hobj , _ := svcS3.HeadObject(params)

		fmt.Printf("Filter: %s => %v\n", uTuple.s3Name, hobj.ContentLength == nil)
		return hobj.ContentLength == nil
	//	get the URLs
	}).Map(func(uTuple URLTuple) statusTuple {
		fmt.Printf("Map3: %v\n", uTuple)
		// random sleep
		time.Sleep(time.Duration(uTuple.msWait) * time.Millisecond)

		// get URL
		resp, err := http.Get(uTuple.gURL)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

//		fmt.Println("Body:", resp.Body)
//		fmt.Println("Proto:", resp.Proto)
//		fmt.Printf("response Status = <%s> / Length = %d\n", resp.Status, resp.ContentLength)
//		fmt.Println("response Headers:", resp.Header)
//		fmt.Printf("response %+v:\n", resp)
//		fmt.Println("response Body:", string(body))
		failed := 0
		passed := 0
		if(resp.StatusCode == 200) {
			passed = 1
		} else {
			failed = 1
		}
		// store in S3
		if(passed == 1) {
			body, _ := ioutil.ReadAll(resp.Body)
			reader := strings.NewReader(string(body))
			root, err := html.Parse(reader)

			if err != nil {
				fmt.Printf("%+v\n", err)
			}

			var b bytes.Buffer
			html.Render(&b, root)
			fixedHtml := b.String()


			isOk := func(r rune) bool {
				return r < 32 || r >= 127
			}
			// The isOk filter is such that there is no need to chain to norm.NFC
			t2 := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
			// This Transformer could also trivially be applied as an io.Reader
			// or io.Writer filter to automatically do such filtering when reading
			// or writing data anywhere.
			fixedUnicodeNFKD, _, _ := transform.String(t2, fixedHtml)

//			fmt.Println("\n\n\n"+fixedUnicodeNFKD)
			reader = strings.NewReader(fixedUnicodeNFKD)

			xmlroot, xmlerr := xmlpath.ParseHTML(reader)
			if xmlerr != nil {
				log.Fatal(xmlerr)
			}
			//	fmt.Printf("xml root = %+v\n------\n", xmlroot)
			path := &xmlpath.Path{}
			pstr := string("")

			pstr = `/html/head/title`
			path = xmlpath.MustCompile(pstr)
			var ok bool

			title := ""
			if title, ok = path.String(xmlroot); ok {
				//		fmt.Printf("%s: %s\n", pstr, title)
			}
			fmt.Printf("**** Title: %s\n", title)
			var iter *xmlpath.Iter
			var list *xmlpath.Path
			var cnt int

			// Location - needs Trim
			pstr = `//*[@id="hed"]/ul[1]/li/text()`
			path = xmlpath.MustCompile(pstr)
			location := ""
			if location, ok = path.String(xmlroot); ok {
				//		fmt.Printf("Location - %s: %s\n", pstr, strings.Trim(location, " \n"))
				location = strings.Trim(location, " \n")
			}

			// Base Skills - LOOP from 1 until not ok
			var skills []string

			list = xmlpath.MustCompile(`//*[@id="hed"]/div[2]/p/a`)
			iter = list.Iter(xmlroot)
			for iter.Next() {
				ele := iter.Node().String()
				skills = append(skills, ele)
				//		fmt.Printf("Sk-Desc: %s\n", ele)
			}


			var desc []string
			list = xmlpath.MustCompile(`//*[@id="jobdetailpage"]/div[2]/div[1]/div[2]/p`)
			iter = list.Iter(xmlroot)
			for iter.Next() {
				ele := iter.Node().String()
				desc = append(desc, ele)
				//		fmt.Printf("it-Desc1: %s\n", ele)
			}

			list = xmlpath.MustCompile(`//*[@id="jobdetailpage"]/div[2]/div[1]/div[2]/ul/li`)
			iter = list.Iter(xmlroot)
			for iter.Next() {
				ele := iter.Node().String()
				desc = append(desc, ele)
				//		fmt.Printf("it-Desc2: %s\n", ele)
			}


			var sSNR []string
			list = xmlpath.MustCompile(`//*[@id="jobdetailpage"]/div[2]/div[1]/div[3]/p`)
			iter = list.Iter(xmlroot)
			cnt = 0
			for iter.Next() {
				ele := iter.Node().String()
				sSNR = append(sSNR, ele)
				//		fmt.Printf("Skills1 (%d): %s\n", cnt, ele)
				cnt++
			}

			list = xmlpath.MustCompile(`//*[@id="jobdetailpage"]/div[2]/div[1]/div[3]/ul/li/text()`)
			iter = list.Iter(xmlroot)
			cnt = 0
			for iter.Next() {
				ele := iter.Node().String()
				sSNR = append(sSNR, ele)
				//		fmt.Printf("Skills2(%d): %s\n", cnt, ele)
				cnt++
			}

			list = xmlpath.MustCompile(`//*[@id="jobdetailpage"]/div[2]/div[1]/div[3]/ul/li/ul/li/text()`)
			iter = list.Iter(xmlroot)
			cnt = 0
			for iter.Next() {
				ele := iter.Node().String()
				sSNR = append(sSNR, ele)
				//		fmt.Printf("Skills3(%d): %s\n", cnt, ele)
				cnt++
			}
			//
			//    // about company -
			//	pstr = `//*[@id="jobdetailpage"]/div[2]/div[1]/div[4]/p/text()`
			//	//*[@id="jobdetailpage"]/div[2]/div[1]/div[2]/p[2]/text()[1]
			//	path = xmlpath.MustCompile(pstr)
			//	about := ""
			//	if about, ok = path.String(xmlroot); ok {
			//		fmt.Printf("About: %s - %s\n", pstr, about)
			//	}

			var about []string
			list = xmlpath.MustCompile(`//*[@id="jobdetailpage"]/div[2]/div[1]/div[4]/p`)
			//*[@id="jobdetailpage"]/div[2]/div[1]/div[4]/p[2]/text()[1]
			iter = list.Iter(xmlroot)
			cnt = 0
			for iter.Next() {
				ele := iter.Node().String()
				about = append(about, ele)
				//		fmt.Printf("About(%d): %s\n", cnt, ele)
				cnt++
			}



			var sep string;

			baseAbout := "ABOUT: "
			sep = ""
			for i := 0; i < len(about); i++ {
				baseAbout += sep + about[i]
				sep = "\n"
			}


			baseSkills := "BASESKILLS: "
			sep = ""
			//	fmt.Printf("base skills = %+v\n", skills)
			for i := 0; i < len(skills); i++ {
				baseSkills += sep + skills[i]
				sep = " "
			}

			baseReqs := "REQUIREMENTS: "
			sep = ""
			for i := 0; i < len(sSNR); i++ {
				baseReqs += sep + sSNR[i]
				sep = "\n"
			}

			baseDesc := "DESCRIPTION: "
			sep = ""
			for i := 0; i < len(desc); i++ {
				baseDesc += sep + desc[i]
				sep = "\n"
			}

			var storage string
			storage =
				uTuple.gURL +"\n\n" +
				"DATE: " + time.Now().Format(time.RFC850) + "\n\n" +
				"TITLE: " + html.UnescapeString(title) + "\n\n" +
				"LOCATION: " + html.UnescapeString(location) + "\n\n" +
				html.UnescapeString(baseSkills) + "\n\n" +
				html.UnescapeString(baseAbout) + "\n\n" +
				html.UnescapeString(baseDesc) + "\n\n" + // no second slash
				html.UnescapeString(baseReqs) +"\n"

			fmt.Printf("Storing (len = %d):\n***\n%s\n***\n", len(storage), storage)

			svcS3 := s3.New(session.New(&aws.Config{Region: aws.String("us-east-1")}))
			bucket := "opps"
			key := uTuple.s3Name
			_, err = svcS3.PutObject(&s3.PutObjectInput{
				Body:   strings.NewReader(string(storage)),
				Bucket: &bucket,
				Key:    &key,
			})
			if err != nil {
				fmt.Printf("Failed to upload data to %s/%s, %s\n", bucket, key, err)
				failed = 1
				passed = 0
			}
		}
//		return statusTuple{passed, failed}
		return statusTuple{passed, failed}
// count URLs
	}).Reduce(func(x statusTuple, y statusTuple) statusTuple {
		fmt.Printf("Red1: x= %v, y = %v\n",x,y )
		return statusTuple{x.pass + y.pass, x.fail + y.fail}
	}).Map(func(x statusTuple) {
		fmt.Printf("Map4 Result: passed = %d, failed = %d\n", x.pass, x.fail)
	}).Run()

}

