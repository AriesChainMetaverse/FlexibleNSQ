package fnsq

type manageServer manage

type Server interface {
	Start(serverName string) Worker
	Stop()
	Wait()
	Publisher(pub Publisher)
	PublishMessage(topic string, message []byte)
}

func (m *manageServer) Publisher(pub Publisher) {
	m.manage().Publisher(pub)
}

func (m *manageServer) Start(serverName string) Worker {
	w := m.manage().register(m.config.RegisterName, serverName)
	m.manage().start()
	return w
}

func (m *manageServer) Stop() {
	m.manage().Stop()
}

func (m *manageServer) Wait() {
	m.manage().Wait()
}

func (m *manageServer) PublishMessage(topic string, message []byte) {
	m.manage().PublishMessage(topic, message)
}

func (m *manageServer) manage() *manage {
	return (*manage)(m)
}

var _ Server = (*manageServer)(nil)
