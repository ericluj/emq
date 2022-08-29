package diskqueue

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	log "github.com/ericluj/elog"
)

type Diskqueue struct {
	mtx      sync.RWMutex
	exitChan chan int

	name        string
	dataPath    string
	needSync    bool          // 是否需要sync
	syncEvery   int64         // 每多少个写执行sync
	syncTimeout time.Duration // 每多少时间执行sync

	readFileNum  int64
	writeFileNum int64
	readPos      int64
	writePos     int64
	depth        int64

	writeChan chan []byte
	readChan  chan []byte
	readFile  *os.File
	writeFile *os.File
}

func New(name, dataPath string, syncEvery int64, syncTimeout time.Duration) *Diskqueue {
	d := &Diskqueue{
		name:        name,
		dataPath:    dataPath,
		syncEvery:   syncEvery,
		syncTimeout: syncTimeout,
		writeChan:   make(chan []byte),
		readChan:    make(chan []byte),
		exitChan:    make(chan int),
	}
	err := d.retrieveMetadata()
	if err != nil && !os.IsNotExist(err) {
		log.Infof("retrieveMetadata error: %v, name: %s", err, d.name)
	}
	go d.ioLoop()
	return d
}

func (d *Diskqueue) retrieveMetadata() error {
	return nil
}

func (d *Diskqueue) ioLoop() {
	var (
		err      error
		dataRead []byte
		count    int64 // 写次数记录
		r        chan []byte
	)
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// 写次数是否达到了sync
		if count == d.syncEvery {
			d.needSync = true
		}

		// 执行sync
		if d.needSync {
			err = d.sync()
			if err != nil {
				log.Infof("sync error: %v, name: %s", err, d.name)
			}
			count = 0
		}

		// 如果有没有读取的数据
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			dataRead, err = d.readOne()
			if err != nil {
				log.Infof("readOne error: %v, name: %s, fileName: %s, readPos: %d", err, d.name, d.fileName(d.readFileNum), d.readPos)
				continue
			}
			r = d.readChan
		} else {
			// 不读数据，设置为nil（相当于走不到该case）
			r = nil
		}

		select {
		case r <- dataRead:
		case dataWrite := <-d.writeChan:
			count++
			d.writeOne(dataWrite)
		case <-syncTicker.C:
			// 如果有过写，那么执行sync（没有过任何活动不需要sync）
			if count != 0 {
				d.needSync = true
			}
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	syncTicker.Stop()
	log.Infof("ioLoop exit")
}

func (d *Diskqueue) sync() error {
	// data内容sync memory -> disk
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	// 持久化metadata
	err := d.persistMetadata()
	if err != nil {
		return err
	}
	d.needSync = false
	return nil
}

// 持久化读写的指针数据
func (d *Diskqueue) persistMetadata() error {
	var (
		err error
		f   *os.File
	)

	fileName := d.metadataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// 写入临时文件
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n", d.depth, d.readFileNum, d.readPos, d.writeFileNum, d.writePos)
	if err != nil {
		return err
	}
	_ = f.Sync()

	// 重命名文件
	return os.Rename(tmpFileName, fileName)
}

func (d *Diskqueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.data"), d.name, fileNum)
}

func (d *Diskqueue) metadataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.metadata"), d.name)
}

func (d *Diskqueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *Diskqueue) Put(data []byte) error {
	return nil
}

func (d *Diskqueue) readOne() ([]byte, error) {
	return nil, nil
}

func (d *Diskqueue) writeOne([]byte) error {
	return nil
}
