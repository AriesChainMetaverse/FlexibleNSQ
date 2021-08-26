package fnsq

type manageServer manage

func (m *manageServer) Start(serverName string) Worker {
	w := m.manage().register(m.config.RegisterName, serverName)
	m.manage().start()
	return w
}

//func (m *manageServer) registerServer(channel string) Worker {
//	m.manage().PublishWorker(NewPublishWorker(m.config.RegisterName, []byte(HelloWorld)))
//	return m.manage().RegisterConsumeWorker(m.config.RegisterName, channel, 0)
//}

func (m *manageServer) PublicMessage(topic string, message []byte) {
	m.manage().Publish(topic, message)
}

func (m *manageServer) manage() *manage {
	return (*manage)(m)
}

var _ Server = (*manageServer)(nil)
