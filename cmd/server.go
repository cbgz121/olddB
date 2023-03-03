package cmd

import (
	"errors"
	"fmt"
	"log"
	"oldrosedb"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

//用于 cmd 执行的 函数
type ExecCmdFunc func(*oldrosedb.RoseDB, []string) (interface{}, error)

//保存指定命令对应的所有函数
var ExecCmd = make(map[string]ExecCmdFunc)

var (
	nestedMultiErr  = errors.New("ERR MULTI calls can not be nested")
	withoutMultiErr = errors.New("ERR EXEC without MULTI")
	execAbortErr    = errors.New("EXECABORT Transaction discarded because of previous errors")
)

//添加执行命令所用的函数
func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

//一个oldrosedb服务器
type Server struct {
	server   *redcon.Server
	db       *oldrosedb.RoseDB
	closed   bool
	mu       sync.Mutex
	TxnLists sync.Map
}

type TxnList struct {
	cmdArgs [][]string
	err     error
}

//创建一个新的oldrosedb服务器
func NewServer(config oldrosedb.Config) (*Server, error) {
	db, err := oldrosedb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

func NewServerUseDbPtr(db *oldrosedb.RoseDB) *Server {
	return &Server{db: db}
}

//监听服务器
func (s *Server) Listen(addr string) {
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
			s.TxnLists.Delete(conn.RemoteAddr())
		},
	) //创建redcon服务器设置连接和断开的回调函数

	s.server = svr
	log.Println("rosedb is running, ready to accept connections.")
	if err := svr.ListenAndServe(); err != nil { //监听服务器,内部会调用回调函数
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}

//停止服务器
func (s *Server) Stop() {
	if s.closed {
		return
	}
	s.mu.Lock()
	s.closed = true
	if err := s.server.Close(); err != nil { //关闭服务器
		log.Printf("close redcon err: %+v\n", err)
	}
	if err := s.db.Close(); err != nil { //关闭数据库
		log.Printf("close rosedb err: %+v\n", err)
	}
	s.mu.Unlock()
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v", r)
		}
	}()

	var reply interface{}
	var err error

	command := strings.ToLower(string(cmd.Args[0]))
	if command == "multi" {
		if _, ok := s.TxnLists.Load(conn.RemoteAddr()); !ok {

			var txnList TxnList
			s.TxnLists.Store(conn.RemoteAddr(), &txnList)
			reply = "OK"
		} else {
			err = nestedMultiErr
		}
	} else if command == "exec" {
		if value, ok := s.TxnLists.Load(conn.RemoteAddr()); ok {
			s.TxnLists.Delete(conn.RemoteAddr())

			txnList := value.(*TxnList)
			if txnList.err != nil {
				err = execAbortErr
			} else {
				if len(txnList.cmdArgs) == 0 {
					reply = "(empty list or set)"
				} else {
					reply, err = txn(s.db, txnList.cmdArgs)
				}
			}
		} else {
			err = withoutMultiErr
		}
	} else {
		if value, ok := s.TxnLists.Load(conn.RemoteAddr()); ok {
			txnList := value.(*TxnList)
			_, exist := ExecCmd[command]

			if !exist {
				txnList.err = fmt.Errorf("ERR unknown command '%s'", command)
				conn.WriteError(txnList.err.Error())
				return
			}
			txnList.cmdArgs = append(txnList.cmdArgs, parseTxnArgs(cmd.Args))

			reply = "QUEUED"
		} else {
			exec, exist := ExecCmd[command]
			if !exist {
				conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
				return
			}

			args := parseArgs(cmd.Args)
			reply, err = exec(s.db, args)
		}
	}

	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)

}

// parseArgs 解析参数
func parseArgs(cmdArgs [][]byte) []string {
	args := make([]string, 0, len(cmdArgs)-1)
	for i, bytes := range cmdArgs {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}
	return args
}

// parseTxnArgs 解析事务命令
func parseTxnArgs(cmdArgs [][]byte) []string {
	args := make([]string, 0, len(cmdArgs))
	for _, bytes := range cmdArgs {
		args = append(args, string(bytes))
	}
	return args
}
