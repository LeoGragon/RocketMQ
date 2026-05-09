import { useState } from 'react'
import { Layout, Menu, Typography } from 'antd'
import { ClusterOutlined, SendOutlined, RocketOutlined, CustomerServiceOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons'
import Dashboard from './pages/Dashboard'
import SendMessage from './pages/SendMessage'
import Simulation from './pages/Simulation'
import OnlineConsumer from './pages/OnlineConsumer'
import MessageQuery from './pages/MessageQuery'

const { Header, Content, Sider } = Layout
const { Title } = Typography

const MENU_ITEMS = [
  { key: 'cluster', icon: <ClusterOutlined />, label: 'Cluster' },
  { key: 'send', icon: <SendOutlined />, label: 'Send Message' },
  { key: 'simulation', icon: <ThunderboltOutlined />, label: 'Simulation' },
  { key: 'consumer', icon: <CustomerServiceOutlined />, label: 'Online Consumer' },
  { key: 'query', icon: <SearchOutlined />, label: 'Message Query' },
]

function App() {
  const [activeKey, setActiveKey] = useState('cluster')
  const [collapsed, setCollapsed] = useState(false)

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider breakpoint="lg" collapsible onCollapse={setCollapsed}>
        <div style={{ height: 32, margin: 16, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          {collapsed ? (
            <RocketOutlined style={{ fontSize: 24, color: '#fff' }} />
          ) : (
            <Title level={5} style={{ color: '#fff', margin: 0 }}>RocketMQ Console</Title>
          )}
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[activeKey]}
          items={MENU_ITEMS}
          onClick={({ key }) => setActiveKey(key)}
        />
      </Sider>
      <Layout>
        <Header style={{ display: 'flex', alignItems: 'center', padding: '0 24px' }}>
          <Title level={4} style={{ color: '#fff', margin: 0 }}>
            {MENU_ITEMS.find(m => m.key === activeKey)?.label}
          </Title>
        </Header>
        <Content style={{ padding: 24 }}>
          {activeKey === 'cluster' && <Dashboard />}
          {activeKey === 'send' && <SendMessage />}
          {activeKey === 'simulation' && <Simulation />}
          {activeKey === 'consumer' && <OnlineConsumer />}
          {activeKey === 'query' && <MessageQuery />}
        </Content>
      </Layout>
    </Layout>
  )
}

export default App
