package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	api "github.com/xflash-panda/server-client/pkg"

	log "github.com/sirupsen/logrus"
)

type UsersService struct {
	client            *api.Client
	config            *Config
	userManager       *UserManager
	trafficManager    *TrafficManager
	connectionManager *ConnectionManager
	userList          *[]api.User
	ctx               context.Context
	cancel            context.CancelFunc
	updateMutex       sync.Mutex
}

func NewUsersService(config *Config, client *api.Client) *UsersService {
	ctx, cancel := context.WithCancel(context.Background())
	return &UsersService{
		client:            client,
		config:            config,
		userManager:       newUserManager(),
		trafficManager:    newTrafficManager(),
		connectionManager: newConnectionManager(),
		ctx:               ctx,
		cancel:            cancel,
	}
}

func (s *UsersService) init() error {
	if s.config.RegisterID == "" {
		return errors.New("register id is not set")
	}
	userList, err := s.client.Users(s.ctx, s.config.RegisterID, api.AnyTLS)
	if err != nil {
		return err
	}
	s.userList = userList
	s.userManager.addUsers(*userList)
	log.Infof("Added %d new users", len(*userList))
	return nil
}

func (s *UsersService) Start() error {
	if err := s.init(); err != nil {
		return err
	}
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		ticker := time.NewTicker(s.config.FetchUserInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.FetchUsersTask(); err != nil {
					log.Errorln("fetch users task error:", err)
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		ticker := time.NewTicker(s.config.ReportTrafficInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.ReportTrafficsTask(); err != nil {
					log.Errorln("report traffic task error:", err)
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		ticker := time.NewTicker(s.config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.HeartbeatTask(); err != nil {
					log.Errorln("heartbeat task error:", err)
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()

	log.Infoln("Start fetch users task")
	log.Infoln("Start report traffic task")
	log.Infoln("Start heartbeat task")
	return nil
}

func (s *UsersService) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *UsersService) Auth(hexString string) bool {
	return s.userManager.auth(hexString)
}

func (s *UsersService) GetUserId(uuidBytes []byte) (int, bool) {
	return s.userManager.GetUserId(uuidBytes)
}

// RegisterConnection registers a connection for a user
func (s *UsersService) RegisterConnection(userId int, conn io.Closer) {
	s.connectionManager.Add(userId, conn)
}

// UnregisterConnection unregisters a connection for a user
func (s *UsersService) UnregisterConnection(userId int, conn io.Closer) {
	s.connectionManager.Remove(userId, conn)
}

func (s *UsersService) UpdateTraffic(userId int, up, down, count uint64) {
	if trafficItem := s.GetTrafficItem(userId); trafficItem != nil {
		if up > 0 {
			trafficItem.Up.Add(up)
		}
		if down > 0 {
			trafficItem.Down.Add(down)
		}
		if count > 0 {
			trafficItem.Count.Add(count)
		}
	}
}

func (s *UsersService) FetchUsersTask() error {
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()

	newUserList, err := s.client.Users(s.ctx, s.config.RegisterID, api.AnyTLS)
	if err != nil {
		if errors.Is(err, api.ErrorUserNotModified) {
			log.Infoln("user not modified:", err)
		} else {
			log.Errorln("fetch users task error:", err)
		}
		return nil
	}

	deleted, added := s.compareUserList(newUserList)

	if len(deleted) > 0 {
		s.userManager.deleteUsers(deleted, s.connectionManager)
	}

	if len(added) > 0 {
		s.userManager.addUsers(added)
	}

	log.Infof("%d user deleted, %d user added", len(deleted), len(added))
	log.Infof("current users: %d", s.userManager.countUsers())
	s.userList = newUserList
	return nil
}

func (s *UsersService) toUserTraffics() []*api.UserTraffic {
	return s.trafficManager.toUserTraffics()
}

func (s *UsersService) ReportTrafficsTask() error {
	userTraffics := s.toUserTraffics()
	log.Infof("%d user traffic needs to be reported", len(userTraffics))
	if len(userTraffics) > 0 {
		err := s.client.Submit(s.ctx, s.config.RegisterID, api.AnyTLS, userTraffics)
		if err != nil {
			log.Errorln("report traffics task error:", err)
			return nil
		}
		s.trafficManager.clear()
	}
	return nil
}

func (s *UsersService) HeartbeatTask() error {
	log.Infoln("heartbeat task")
	if err := s.client.Heartbeat(s.ctx, s.config.RegisterID, api.AnyTLS, ""); err != nil {
		return err
	}
	return nil
}

func (s *UsersService) compareUserList(newUsers *[]api.User) (deleted, added []api.User) {
	oldMap := make(map[string]api.User)
	newMap := make(map[string]api.User)

	for _, user := range *s.userList {
		oldMap[user.UUID] = user
	}
	for _, user := range *newUsers {
		newMap[user.UUID] = user
	}

	for uuid, user := range oldMap {
		if _, exists := newMap[uuid]; !exists {
			deleted = append(deleted, user)
		}
	}

	for uuid, user := range newMap {
		if _, exists := oldMap[uuid]; !exists {
			added = append(added, user)
		}
	}

	return deleted, added
}

func (s *UsersService) GetTrafficItem(userId int) *TrafficItem {
	return s.trafficManager.loadOrStore(userId)
}

type UserManager struct {
	store sync.Map
}

func newUserManager() *UserManager {
	return &UserManager{store: sync.Map{}}
}

func (um *UserManager) GetUserId(uuidBytes []byte) (int, bool) {
	if data, ok := um.store.Load(hex.EncodeToString(uuidBytes)); ok {
		if user, ok := data.(*api.User); ok {
			return user.ID, true
		}
	}
	return 0, false
}

func (um *UserManager) addUsers(users []api.User) {
	for _, user := range users {
		key := sha256.Sum256([]byte(user.UUID))
		um.store.Store(hex.EncodeToString(key[:]), &user)
		log.Debugf("add user uuid %s, id %d", user.UUID, user.ID)
	}
}

func (um *UserManager) deleteUsers(users []api.User, cm *ConnectionManager) {
	for _, user := range users {
		key := sha256.Sum256([]byte(user.UUID))
		um.store.Delete(hex.EncodeToString(key[:]))
		log.Debugf("delete user uuid %s, id %d", user.UUID, user.ID)
		// Close all connections for this user
		cm.CloseAll(user.ID)
	}
}

func (um *UserManager) countUsers() int {
	length := 0
	um.store.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func (um *UserManager) auth(hexString string) bool {
	_, ok := um.store.Load(hexString)
	return ok
}

type TrafficManager struct {
	store sync.Map
}

func (tm *TrafficManager) toUserTraffics() []*api.UserTraffic {
	userTraffics := make([]*api.UserTraffic, 0)
	tm.store.Range(func(key, value any) bool {
		userId, ok := key.(int)
		if !ok {
			return false
		}
		trafficItem := value.(*TrafficItem)
		if trafficItem.Up.Value() > 0 || trafficItem.Down.Value() > 0 || trafficItem.Count.Value() > 0 {
			userTraffics = append(userTraffics, &api.UserTraffic{
				UID:      userId,
				Upload:   trafficItem.Up.Value(),
				Download: trafficItem.Down.Value(),
				Count:    trafficItem.Count.Value(),
			})
		}
		return true
	})
	return userTraffics
}

func (tm *TrafficManager) loadOrStore(userId int) *TrafficItem {
	if item, ok := tm.store.Load(userId); ok {
		return item.(*TrafficItem)
	}
	item, _ := tm.store.LoadOrStore(userId, newTrafficItem())
	return item.(*TrafficItem)
}

func (tm *TrafficManager) clear() {
	tm.store.Range(func(key interface{}, value interface{}) bool {
		value.(*TrafficItem).delete()
		return true
	})
}

func newTrafficManager() *TrafficManager {
	return &TrafficManager{store: sync.Map{}}
}

func NewExportedTrafficManager() *TrafficManager {
	return newTrafficManager()
}

func NewUsersServiceWithTrafficManager(tm *TrafficManager) *UsersService {
	return &UsersService{
		trafficManager: tm,
	}
}

type TrafficItem struct {
	Up    *Counter
	Down  *Counter
	Count *Counter
}

func (t *TrafficItem) delete() {
	t.Count.Reset()
	t.Down.Reset()
	t.Up.Reset()
}

func newTrafficItem() *TrafficItem {
	return &TrafficItem{NewCounter(0), NewCounter(0), NewCounter(0)}
}

// ConnectionManager manages user to connection mapping
type ConnectionManager struct {
	mu    sync.RWMutex
	conns map[int]map[io.Closer]struct{}
}

func newConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		conns: make(map[int]map[io.Closer]struct{}),
	}
}

func (cm *ConnectionManager) Add(userId int, conn io.Closer) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.conns[userId] == nil {
		cm.conns[userId] = make(map[io.Closer]struct{})
	}
	cm.conns[userId][conn] = struct{}{}
}

func (cm *ConnectionManager) Remove(userId int, conn io.Closer) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.conns[userId] != nil {
		delete(cm.conns[userId], conn)
		if len(cm.conns[userId]) == 0 {
			delete(cm.conns, userId)
		}
	}
}

func (cm *ConnectionManager) CloseAll(userId int) {
	cm.mu.Lock()
	conns := cm.conns[userId]
	delete(cm.conns, userId)
	cm.mu.Unlock()

	for conn := range conns {
		_ = conn.Close()
	}
	if len(conns) > 0 {
		log.Debugf("closed %d connections for user %d", len(conns), userId)
	}
}
