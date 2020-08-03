package mocks

import (
	"github.com/lyft/flyteadmin/pkg/runtime/interfaces"
)

type MockApplicationProvider struct {
	dbConfig            interfaces.DbConfig
	topLevelConfig      interfaces.ApplicationConfig
	schedulerConfig     interfaces.SchedulerConfig
	remoteDataConfig    interfaces.RemoteDataConfig
	notificationsConfig interfaces.NotificationsConfig
	domainsConfig       interfaces.DomainsConfig
}

func (p *MockApplicationProvider) GetDbConfig() interfaces.DbConfig {
	return p.dbConfig
}

func (p *MockApplicationProvider) SetDbConfig(dbConfig interfaces.DbConfig) {
	p.dbConfig = dbConfig
}

func (p *MockApplicationProvider) GetTopLevelConfig() *interfaces.ApplicationConfig {
	return &p.topLevelConfig
}

func (p *MockApplicationProvider) SetTopLevelConfig(topLevelConfig interfaces.ApplicationConfig) {
	p.topLevelConfig = topLevelConfig
}

func (p *MockApplicationProvider) GetSchedulerConfig() *interfaces.SchedulerConfig {
	return &p.schedulerConfig
}

func (p *MockApplicationProvider) SetSchedulerConfig(schedulerConfig interfaces.SchedulerConfig) {
	p.schedulerConfig = schedulerConfig
}

func (p *MockApplicationProvider) GetRemoteDataConfig() *interfaces.RemoteDataConfig {
	return &p.remoteDataConfig
}

func (p *MockApplicationProvider) SetRemoteDataConfig(remoteDataConfig interfaces.RemoteDataConfig) {
	p.remoteDataConfig = remoteDataConfig
}

func (p *MockApplicationProvider) GetNotificationsConfig() *interfaces.NotificationsConfig {
	return &p.notificationsConfig
}

func (p *MockApplicationProvider) SetNotificationsConfig(notificationsConfig interfaces.NotificationsConfig) {
	p.notificationsConfig = notificationsConfig
}

func (p *MockApplicationProvider) GetDomainsConfig() *interfaces.DomainsConfig {
	return &p.domainsConfig
}

func (p *MockApplicationProvider) SetDomainsConfig(domainsConfig interfaces.DomainsConfig) {
	p.domainsConfig = domainsConfig
}