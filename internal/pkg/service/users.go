package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	pb "github.com/xflash-panda/server-agent-proto/pkg"
	api "github.com/xflash-panda/server-client/pkg"

	log "github.com/sirupsen/logrus"
)

type UsersService struct {
	client            pb.AgentClient
	config            *Config
	userManager       *UserManager
	trafficManager    *TrafficManager
	connectionManager *ConnectionManager
	userList          *[]api.User
	ctx               context.Context
	cancel            context.CancelFunc
	updateMutex       sync.Mutex
	registerID        string
}

// SetRegisterInfo sets the register id for agent communication
func (s *UsersService) SetRegisterInfo(registerID string) {
	s.registerID = registerID
}

func NewUsersService(config *Config, client pb.AgentClient) *UsersService {
	ctx, cancel := context.WithCancel(context.Background())
	return &UsersService{
		client:            client,
		config:            config,
		userManager:       newUserManager(),
		trafficManager:    newTrafficManager(),
		connectionManager: newConnectionManager(),
		ctx:               ctx,
		cancel:            cancel,
		registerID:        "",
	}
}

func (s *UsersService) init() error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	r, err := s.client.Users(ctx, &pb.UsersRequest{NodeType: pb.NodeType_ANYTLS, NodeId: int32(s.config.NodeID)})
	if err != nil {
		return err
	}
	raw := r.GetRawData()
	if len(raw) == 0 {
		// Keep existing user list unchanged when agent returns empty data
		log.Infoln("init users: raw data is empty, keep current users")
		return nil
	}
	userList, err := api.UnmarshalUsers(raw)
	if err != nil {
		return err
	}
	s.userList = userList
	s.userManager.addUsers(*userList)
	log.Infof("Added %d new users", len(*s.userList))
	return nil
}

func (s *UsersService) Start() error {
	if err := s.init(); err != nil {
		return fmt.Errorf("failed to initialize users service: %w", err)
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
		ticker := time.NewTicker(s.config.HeartBeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.HeartBeatTask(); err != nil {
					log.Errorln("heartbeat task error:", err)
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()
	log.Infoln("Start fetch users task")
	log.Infoln("Start report traffic task")
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

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	r, err := s.client.Users(ctx, &pb.UsersRequest{NodeType: pb.NodeType_ANYTLS, NodeId: int32(s.config.NodeID)})
	if err != nil {
		log.Errorln("fetch users error:", err)
		return nil
	}
	raw := r.GetRawData()
	if len(raw) == 0 {
		log.Infoln("users raw data is empty, no changes")
		return nil
	}
	newUserList, err := api.UnmarshalUsers(raw)
	if err != nil {
		log.Errorln("unmarshal users error", err)
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

func (s *UsersService) ReportTrafficsTask() error {
	trafficsRawResult, statsRawResult := s.trafficManager.toTrafficsRawData()
	if trafficsRawResult.Err != nil {
		log.Errorln(trafficsRawResult.Err)
		return nil
	}
	if statsRawResult.Err != nil {
		log.Errorln(statsRawResult.Err)
		return nil
	}
	log.Infoln("reporting traffics...")
	if len(statsRawResult.Data) > 0 || len(trafficsRawResult.Data) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()
		_, err := s.client.Submit(ctx, &pb.SubmitRequest{NodeType: pb.NodeType_ANYTLS, RegisterId: s.registerID, RawData: trafficsRawResult.Data, RawStats: statsRawResult.Data})
		if err != nil {
			log.Errorln(err)
		}
		s.trafficManager.clear()
	}
	return nil
}

func (s *UsersService) HeartBeatTask() error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	log.Infoln("heartbeat...")
	_, err := s.client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeType: pb.NodeType_ANYTLS, RegisterId: s.registerID})
	if err != nil {
		log.Errorln(err)
		return nil
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
	item := s.trafficManager.load(userId)
	if item == nil {
		newItem := newTrafficItem()
		s.trafficManager.set(userId, newItem)
		return newItem
	}
	return item
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

func (tm *TrafficManager) load(userId int) *TrafficItem {
	if item, ok := tm.store.Load(userId); !ok {
		return nil
	} else {
		return item.(*TrafficItem)
	}
}

func (tm *TrafficManager) set(userId int, item *TrafficItem) {
	tm.store.Store(userId, item)
}

func (tm *TrafficManager) clear() {
	tm.store.Range(func(key interface{}, value interface{}) bool {
		value.(*TrafficItem).delete()
		return true
	})
}

func (tm *TrafficManager) toTrafficsRawData() (*RawResult, *RawResult) {
	traffics, stats := tm.toTraffics()
	trafficsRawResult := &RawResult{}
	trafficsRawResult.Data, trafficsRawResult.Err = api.MarshalTraffics(traffics)
	statsRawResult := &RawResult{}
	statsRawResult.Data, statsRawResult.Err = api.MarshalTrafficStats(stats)
	return trafficsRawResult, statsRawResult
}

func (tm *TrafficManager) toTraffics() ([]*api.UserTraffic, *api.TrafficStats) {
	userTraffics := make([]*api.UserTraffic, 0)
	i := 0
	var stats api.TrafficStats
	stats.UserIds = make([]int, 0)
	stats.UserRequests = make(map[int]int)
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
		count := int(trafficItem.Count.Value())
		if count > 0 {
			stats.Requests += count
			stats.Count++
			stats.UserIds = append(stats.UserIds, userId)
			stats.UserRequests[userId] = int(trafficItem.Count.Value())
		}
		i++
		return true
	})
	return userTraffics, &stats
}

func newTrafficManager() *TrafficManager {
	return &TrafficManager{store: sync.Map{}}
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
