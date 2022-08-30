package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

	name                string
	dataPath            string
	needSync            bool          // 是否需要sync
	syncEvery           int64         // 每多少个写执行sync
	syncTimeout         time.Duration // 每多少时间执行sync
	maxBytesPerFile     int64         // 文件最大可写字节数
	maxBytesPerFileRead int64         // 文件最大可读字节数
	minMsgSize          int32
	maxMsgSize          int32

	readFileNum  int64
	writeFileNum int64
	readPos      int64
	writePos     int64
	depth        int64 // 没有读过的数据量

	nextReadPos     int64
	nextReadFileNum int64

	writeChan         chan []byte
	writeResponseChan chan error
	readChan          chan []byte
	readFile          *os.File
	writeFile         *os.File
	reader            *bufio.Reader
	writeBuf          bytes.Buffer // 写缓冲区，提升性能
}

func New(name, dataPath string, syncEvery, maxBytesPerFile int64, minMsgSize, maxMsgSize int32, syncTimeout time.Duration) *Diskqueue {
	d := &Diskqueue{
		name:              name,
		dataPath:          dataPath,
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		readChan:          make(chan []byte),
		exitChan:          make(chan int),
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
		// 读写次数是否达到了sync
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

		// 如果有未读取的数据
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			// 本次读取的数据已经被处理完
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					log.Infof("readOne error: %v, name: %s, fileName: %s, readPos: %d", err, d.name, d.fileName(d.readFileNum), d.readPos)
					continue
				}
				r = d.readChan
			}
		} else {
			// 不读数据，设置为nil（相当于走不到该case）
			r = nil
		}

		// 这里的逻辑值得注意，因为读与写都是在一个loop中处理
		select {
		case r <- dataRead:
			count++
			d.moveForward()
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
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
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	d.writeChan <- data

	return <-d.writeResponseChan
}

func (d *Diskqueue) readOne() ([]byte, error) {
	var (
		err     error
		msgSize int32
	)

	// 打开文件
	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		log.Infof("readOne OpenFile name: %s, file: %s", d.name, curFileName)

		// 如果有偏移量，设置读取位置
		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, io.SeekStart)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.maxBytesPerFileRead = d.maxBytesPerFile
		if d.readFileNum < d.writeFileNum {
			stat, err := d.readFile.Stat()
			if err != nil {
				d.maxBytesPerFileRead = stat.Size()
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	// 读数据
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid msg size %d", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// 判断是否读取下一个文件
	if d.readFileNum < d.writeFileNum && d.readPos >= d.maxBytesPerFileRead {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadPos = 0
		d.nextReadFileNum++
	}

	return readBuf, nil
}

func (d *Diskqueue) writeOne(data []byte) error {
	var (
		err error
	)

	msgSize := int32(len(data))
	totalBytes := int64(4 + msgSize)

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		return fmt.Errorf("invalid msg size %d", msgSize)
	}

	// 如果写入内容超过文件最大容量
	if d.writePos > 0 && d.writePos+totalBytes > d.maxBytesPerFile {
		// 如果读写的是同一个文件，那么最大可读字节数设置
		if d.readFileNum == d.writeFileNum {
			d.maxBytesPerFileRead = d.writePos
		}

		d.writePos = 0
		d.writeFileNum++

		// 每次写新文件前，sync缓冲区内容
		err = d.sync()
		if err != nil {
			log.Infof("sync error: %v", err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	// 创建新文件
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		log.Infof("writeOne OpenFile name: %s, file: %s", d.name, curFileName)

		// 如果有偏移量，设置写入位置
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, io.SeekStart)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	// 写数据
	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, msgSize)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	d.writePos += totalBytes
	d.depth++

	return nil
}

func (d *Diskqueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readPos = d.nextReadPos
	d.readFileNum = d.nextReadFileNum
	d.depth--

	// 删除旧文件
	if oldReadFileNum != d.nextReadFileNum {
		d.needSync = true

		removeFileName := d.fileName(oldReadFileNum)
		err := os.Remove(removeFileName)
		if err != nil {
			log.Infof("moveForward error: %v, name: %s, fileName: %s", err, d.name, removeFileName)
		}
	}
}
