import { useEffect, useState } from 'react'
import {
  Card, Table, Tag, Statistic, Row, Col, message, Tabs,
  Button, Modal, Form, Input, InputNumber, Switch, Space,
} from 'antd'
import {
  ClusterOutlined, CheckCircleOutlined, CloseCircleOutlined,
  NodeIndexOutlined, MessageOutlined, PlusOutlined,
} from '@ant-design/icons'

interface BrokerInfo {
  cluster: string
  brokerName: string
  addresses: string[]
}

interface TopicStat {
  topic: string
  offsetTable: Record<string, any>
}

export default function Dashboard() {
  const [health, setHealth] = useState<string | null>(null)
  const [clusterInfo, setClusterInfo] = useState<any>({})
  const [brokers, setBrokers] = useState<BrokerInfo[]>([])
  const [topics, setTopics] = useState<string[]>([])
  const [topicStats, setTopicStats] = useState<TopicStat | null>(null)
  const [loadingTopics, setLoadingTopics] = useState(false)
  const [createModalOpen, setCreateModalOpen] = useState(false)
  const [creating, setCreating] = useState(false)
  const [showSystemTopics, setShowSystemTopics] = useState(false)
  const [form] = Form.useForm()

  const isSystemTopic = (topic: string): boolean => {
    if (topic === 'TBW102' || topic === 'BenchmarkTest' || topic === 'SELF_TEST_TOPIC' || topic === 'OFFSET_MOVED_EVENT') return true
    if (topic.startsWith('SCHEDULE_TOPIC_')) return true
    if (topic.startsWith('rmq_sys_') || topic.startsWith('RMQ_SYS_')) return true
    if (topic.includes('DefaultCluster')) return true
    // Broker name hash as topic (e.g. 83fe0c995a82)
    if (brokers.some(b => b.brokerName === topic)) return true
    return false
  }

  const visibleTopics = showSystemTopics ? topics : topics.filter(t => !isSystemTopic(t))

  const fetchApi = async (path: string, opts?: RequestInit) => {
    const res = await fetch(path, opts)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  }

  const loadTopics = () => {
    setLoadingTopics(true)
    fetchApi('/api/topics')
      .then(data => setTopics(data.topics || []))
      .catch(() => {})
      .finally(() => setLoadingTopics(false))
  }

  const loadAll = () => {
    fetchApi('/api/health')
      .then(data => setHealth(data.rocketmq === 'connected' ? 'ok' : 'err'))
      .catch(() => setHealth('err'))

    fetchApi('/api/cluster').then(setClusterInfo).catch(() => {})

    fetchApi('/api/brokers')
      .then(data => setBrokers(data.details || []))
      .catch(() => {})

    loadTopics()
  }

  const handleCreateTopic = async (values: { topic: string; queueNum: number }) => {
    setCreating(true)
    try {
      const data = await fetchApi('/api/topic', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(values),
      })
      if (data.success) {
        message.success(`Topic "${values.topic}" created`)
        setCreateModalOpen(false)
        form.resetFields()
        loadTopics()
      } else {
        message.error(data.error || 'Failed to create topic')
      }
    } catch (err: any) {
      message.error('Failed to create topic: ' + err.message)
    } finally {
      setCreating(false)
    }
  }

  useEffect(() => { loadAll() }, [])

  const handleTopicClick = (topic: string) => {
    fetchApi(`/api/topic/${encodeURIComponent(topic)}/stats`)
      .then(data => setTopicStats(data))
      .catch(err => message.error('Failed to load topic stats: ' + err.message))
  }

  const brokerColumns = [
    { title: 'Broker', dataIndex: 'brokerName', key: 'brokerName' },
    { title: 'Cluster', dataIndex: 'cluster', key: 'cluster' },
    {
      title: 'Addresses', dataIndex: 'addresses', key: 'addresses',
      render: (addrs: string[]) => addrs.map((a, i) => <Tag key={i} color="blue">{a}</Tag>),
    },
  ]

  const topicColumns = [
    {
      title: 'Topic', dataIndex: 'topic', key: 'topic',
      render: (t: string) => <a onClick={() => handleTopicClick(t)}>{t}</a>,
    },
  ]

  return (
    <>
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
          <Col xs={24} sm={8}>
            <Card>
              <Statistic
                title="Namesrv Status"
                valueRender={() =>
                  health === 'ok'
                    ? <Tag icon={<CheckCircleOutlined />} color="success">Connected</Tag>
                    : health === 'err'
                      ? <Tag icon={<CloseCircleOutlined />} color="error">Disconnected</Tag>
                      : <Tag>Checking...</Tag>
                }
              />
            </Card>
          </Col>
          <Col xs={24} sm={8}>
            <Card>
              <Statistic
                title="Brokers"
                prefix={<ClusterOutlined />}
                value={clusterInfo.brokerCount ?? '-'}
              />
            </Card>
          </Col>
          <Col xs={24} sm={8}>
            <Card>
              <Statistic
                title="Namesrv Addr"
                prefix={<NodeIndexOutlined />}
                value={clusterInfo.namesrvAddr ?? '-'}
              />
            </Card>
          </Col>
        </Row>

        <Card title="Brokers" style={{ marginBottom: 24 }}>
          <Table
            dataSource={brokers}
            columns={brokerColumns}
            rowKey="brokerName"
            pagination={false}
            size="middle"
          />
        </Card>

        <Card
          title={<><MessageOutlined /> Topics ({visibleTopics.length}{visibleTopics.length !== topics.length ? ` / ${topics.length}` : ''})</>}
          extra={
            <Space>
              <span style={{ fontSize: 12, color: '#888' }}>Show System</span>
              <Switch size="small" checked={showSystemTopics} onChange={setShowSystemTopics} />
              <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalOpen(true)}>Create Topic</Button>
            </Space>
          }
        >
          <Tabs
            tabPosition="left"
            items={[{
              key: 'list',
              label: 'All Topics',
              children: (
                <Table
                  dataSource={visibleTopics.map(t => ({ topic: t }))}
                  columns={topicColumns}
                  rowKey="topic"
                  pagination={false}
                  loading={loadingTopics}
                  size="middle"
                />
              ),
            }]}
          />
          {topicStats && (
            <Card title={`Topic: ${topicStats.topic}`} style={{ marginTop: 16 }}>
              <pre style={{ maxHeight: 400, overflow: 'auto', fontSize: 12 }}>
                {JSON.stringify(topicStats.offsetTable, null, 2)}
              </pre>
            </Card>
          )}
        </Card>

        <Modal
          title="Create Topic"
          open={createModalOpen}
          onCancel={() => { setCreateModalOpen(false); form.resetFields() }}
          footer={null}
          destroyOnClose
        >
          <Form form={form} layout="vertical" onFinish={handleCreateTopic} initialValues={{ queueNum: 8 }}>
            <Form.Item name="topic" label="Topic Name" rules={[{ required: true, message: 'Please input topic name' }]}>
              <Input placeholder="e.g. my-topic" />
            </Form.Item>
            <Form.Item name="queueNum" label="Queue Number" rules={[{ required: true, type: 'number', min: 1 }]}>
              <InputNumber min={1} max={64} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item>
              <Button type="primary" htmlType="submit" loading={creating} block>Create</Button>
            </Form.Item>
          </Form>
        </Modal>
    </>
  )
}
