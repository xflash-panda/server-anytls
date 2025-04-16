package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	api "github.com/xflash-panda/server-client/pkg"
)

type UsersService struct {
	client         *api.Client
	config         *Config
	userManager    *UserManager
	trafficManager *TrafficManager
	userList       *[]api.User
	ctx            context.Context
	cancel         context.CancelFunc
	updateMutex    sync.Mutex
}

func NewUsersService(config *Config, client *api.Client) *UsersService {
	ctx, cancel := context.WithCancel(context.Background())
	return &UsersService{
		client:         client,
		config:         config,
		userManager:    newUserManager(),
		trafficManager: newTrafficManager(),
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (s *UsersService) init() error {
	userList, _, err := s.client.Users(api.NodeId(s.config.NodeID), api.AnyTLS)
	if err != nil {
		return err
	}
	s.userList = userList
	s.userManager.addUsers(*userList)
	log.Infof("Added %d new users", len(*userList))
	return nil
}

func (s *UsersService) Start() error {
	s.init()
	go func() {
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

func (s *UsersService) UpdateTraffic(userId int, up, down uint64) {
	if trafficItem := s.GetTrafficItem(userId); trafficItem != nil {
		if up > 0 {
			trafficItem.Up.Add(up)
		}
		if down > 0 {
			trafficItem.Down.Add(down)
		}
	}
}

func (s *UsersService) FetchUsersTask() error {
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()

	newUserList, _, err := s.client.Users(api.NodeId(s.config.NodeID), api.AnyTLS)
	if err != nil {
		if errors.Is(err, api.ErrorUserNotModified) {
			log.Infoln(err)
		} else {
			log.Errorln(err)
		}
		return nil
	}

	deleted, added := s.compareUserList(newUserList)

	if len(deleted) > 0 {
		s.userManager.deleteUsers(deleted)
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
		err := s.client.Submit(api.NodeId(s.config.NodeID), api.AnyTLS, userTraffics)
		if err != nil {
			log.Errorln(err)
			return nil
		}
		s.trafficManager.clear()
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

func (um *UserManager) deleteUsers(users []api.User) {
	for _, user := range users {
		key := sha256.Sum256([]byte(user.UUID))
		um.store.Delete(hex.EncodeToString(key[:]))
		log.Debugf("delete user uuid %s, id %d", user.UUID, user.ID)
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
		var userId, ok = key.(int)
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

func (tm *TrafficManager) forRange(f func(key, value any) bool) {
	tm.store.Range(f)
}

func (tm *TrafficManager) delete(userId int) {
	tm.store.Delete(userId)
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
