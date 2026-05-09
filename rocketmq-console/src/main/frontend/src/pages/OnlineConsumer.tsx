import { useEffect, useRef, useState } from 'react'
import {
  Card, Form, Input, Select, Button, Row, Col, Tag, Table,
  Typography, Space, Statistic, Switch, InputNumber,
} from 'antd'
import {
  PlayCircleOutlined, PauseCircleOutlined, ClearOutlined,
  SyncOutlined,
} from '@ant-design/icons'

const { Text } = Typography

interface MessageRecord {
  msgId: string
  topic: string
  tags: string
  keys: string
  body: string
  queueId: number
  queueOffset: number
  bornTimestamp: number
  storeTimestamp: number
  reconsumeTimes: number
}

export default function OnlineConsumer() {
  const [form] = Form.useForm()
  const [topics, setTopics] = useState<string[]>([])
  const [loadingTopics, setLoadingTopics] = useState(false)
  const [running, setRunning] = useState(false)
  const [sessionKey, setSessionKey] = useState<string | null>(null)
  const [messages, setMessages] = useState<MessageRecord[]>([])
  const [totalConsumed, setTotalConsumed] = useState(0)
  const [autoScroll, setAutoScroll] = useState(true)
  const [pollInterval, setPollInterval] = useState(1000)
  const timerRef = useRef<number | null>(null)
  const tableRef = useRef<HTMLDivElement | null>(null)

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

  useEffect(() => { loadTopics() }, [])

  const doPoll = async (key: string) => {
    try {
      const data = await fetchApi('/api/consumer/poll', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionKey: key }),
      })
      if (data.success) {
        const newMsgs: MessageRecord[] = data.messages || []
        if (newMsgs.length > 0) {
          setMessages(prev => [...prev, ...newMsgs])
        }
        setTotalConsumed(data.totalConsumed ?? 0)
      }
    } catch {
      // poll timeout or error — continue
    }
  }

  const handleStart = async (values: { topic: string; consumerGroup: string; tagFilter?: string; pollInterval?: number }) => {
    try {
      const data = await fetchApi('/api/consumer/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(values),
      })
      if (data.success) {
        const key = data.sessionKey as string
        const interval = values.pollInterval ?? pollInterval
        setSessionKey(key)
        setRunning(true)
        setPollInterval(interval)
        setMessages([])
        setTotalConsumed(0)
        doPoll(key)
        timerRef.current = window.setInterval(() => doPoll(key), interval)
      } else {
        const { message } = await import('antd')
        message.error(data.error || 'Failed to start consumer')
      }
    } catch (err: any) {
      const { message } = await import('antd')
      message.error('Failed to start consumer: ' + err.message)
    }
  }

  const handleStop = async () => {
    if (timerRef.current) {
      clearInterval(timerRef.current)
      timerRef.current = null
    }
    if (!sessionKey) return
    try {
      await fetchApi('/api/consumer/stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionKey }),
      })
    } catch (err: any) {
      const { message } = await import('antd')
      message.error('Failed to stop consumer: ' + err.message)
    } finally {
      setRunning(false)
      setSessionKey(null)
    }
  }

  const handleClear = () => {
    setMessages([])
    setTotalConsumed(0)
  }

  useEffect(() => {
    return () => {
      if (timerRef.current) {
        clearInterval(timerRef.current)
      }
      if (sessionKey) {
        fetchApi('/api/consumer/stop', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sessionKey }),
        }).catch(() => {})
      }
    }
  }, [])

  const columns = [
    { title: '#', key: 'index', width: 50, render: (_: any, __: any, i: number) => i + 1 },
    {
      title: 'MsgId', dataIndex: 'msgId', key: 'msgId', width: 200,
      render: (v: string) => <Text copyable style={{ fontSize: 12 }}>{v}</Text>,
    },
    { title: 'Tags', dataIndex: 'tags', key: 'tags', width: 80, render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: 'Keys', dataIndex: 'keys', key: 'keys', width: 120, render: (v: string) => v || '-' },
    {
      title: 'Body', dataIndex: 'body', key: 'body', ellipsis: true,
      render: (v: string) => <Text style={{ fontSize: 12, wordBreak: 'break-all' }}>{v}</Text>,
    },
    { title: 'Queue', key: 'queue', width: 90, render: (_: any, r: MessageRecord) => `Q${r.queueId}:${r.queueOffset}` },
    {
      title: 'Time', key: 'time', width: 200,
      render: (_: any, r: MessageRecord) => {
        const d = new Date(r.storeTimestamp)
        return <Text style={{ fontSize: 11 }}>{d.toLocaleString()}</Text>
      },
    },
    { title: 'Retry', dataIndex: 'reconsumeTimes', key: 'reconsumeTimes', width: 60 },
  ]

  return (
    <Row gutter={[24, 24]}>
      <Col xs={24} lg={8}>
        <Card title="Consumer Config">
          <Form
            form={form}
            layout="vertical"
            onFinish={handleStart}
            initialValues={{ pollInterval: 1000 }}
          >
            <Form.Item name="topic" label="Topic" rules={[{ required: true, message: 'Please select a topic' }]}>
              <Select
                showSearch
                placeholder="Select or search a topic"
                loading={loadingTopics}
                options={topics.map(t => ({ value: t, label: t }))}
                filterOption={(input, option) =>
                  (option?.label as string)?.toLowerCase().includes(input.toLowerCase())
                }
                disabled={running}
              />
            </Form.Item>

            <Form.Item name="consumerGroup" label="Consumer Group" rules={[{ required: true, message: 'Please input consumer group' }]}>
              <Input placeholder="e.g. my-consumer-group" disabled={running} />
            </Form.Item>

            <Form.Item name="tagFilter" label="Tag Filter">
              <Input placeholder="e.g. TagA || TagB (default: *)" disabled={running} />
            </Form.Item>

            <Form.Item name="pollInterval" label="Poll Interval (ms)">
              <InputNumber min={500} max={10000} step={500} style={{ width: '100%' }} disabled={running} />
            </Form.Item>

            <Form.Item>
              <Space>
                {!running ? (
                  <Button type="primary" htmlType="submit" icon={<PlayCircleOutlined />}>
                    Start
                  </Button>
                ) : (
                  <Button danger icon={<PauseCircleOutlined />} onClick={handleStop}>
                    Stop
                  </Button>
                )}
                <Button icon={<ClearOutlined />} onClick={handleClear} disabled={running}>
                  Clear
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </Card>

        <Card style={{ marginTop: 16 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Statistic
                title="Status"
                value={running ? 'Consuming' : 'Idle'}
                valueStyle={{ color: running ? '#52c41a' : '#8c8c8c' }}
              />
            </Col>
            <Col span={12}>
              <Statistic title="Messages" value={totalConsumed} prefix={<SyncOutlined spin={running} />} />
            </Col>
          </Row>
        </Card>
      </Col>

      <Col xs={24} lg={16}>
        <Card
          title="Messages"
          extra={
            <Space>
              <span style={{ fontSize: 12, color: '#888' }}>Auto Scroll</span>
              <Switch size="small" checked={autoScroll} onChange={setAutoScroll} />
            </Space>
          }
        >
          <div ref={tableRef}>
            <Table
              dataSource={messages}
              columns={columns}
              rowKey={(r: MessageRecord) => r.msgId}
              pagination={false}
              size="small"
              scroll={{ y: 500 }}
              locale={{ emptyText: running ? 'Waiting for messages...' : 'Not started' }}
            />
          </div>
        </Card>
      </Col>
    </Row>
  )
}
