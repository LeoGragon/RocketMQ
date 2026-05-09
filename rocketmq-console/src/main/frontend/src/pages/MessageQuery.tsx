import { useEffect, useState } from 'react'
import {
  Card, Form, Input, Select, Button, Row, Col, Tag, Table,
  Typography, Space, DatePicker, InputNumber, message, Tabs, Empty,
} from 'antd'
import { SearchOutlined, ClearOutlined, KeyOutlined, NumberOutlined, ClockCircleOutlined } from '@ant-design/icons'

const { Text } = Typography
const { RangePicker } = DatePicker

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
  bornHost: string
  storeHost: string
  reconsumeTimes: number
  msgSize: number
}

type QueryMode = 'key' | 'msgId' | 'time'

export default function MessageQuery() {
  const [topics, setTopics] = useState<string[]>([])
  const [loadingTopics, setLoadingTopics] = useState(false)
  const [searching, setSearching] = useState(false)
  const [messages, setMessages] = useState<MessageRecord[]>([])
  const [queryMode, setQueryMode] = useState<QueryMode>('key')
  const [lastQuery, setLastQuery] = useState<{ mode: string; total: number } | null>(null)

  const [formKey] = Form.useForm()
  const [formMsgId] = Form.useForm()
  const [formTime] = Form.useForm()

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

  const handleQueryByKey = async (values: { topic: string; key: string; maxNum?: number }) => {
    setSearching(true)
    try {
      const endTime = Date.now()
      const beginTime = endTime - 24 * 60 * 60 * 1000 // last 24h by default
      const data = await fetchApi('/api/message/queryByKey', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topic: values.topic,
          key: values.key,
          beginTime,
          endTime,
          maxNum: values.maxNum || 32,
        }),
      })
      if (data.success) {
        setMessages(data.messages || [])
        setLastQuery({ mode: 'Key', total: data.total })
        if ((data.messages || []).length === 0) message.info('No messages found')
      } else {
        message.error(data.error || 'Query failed')
        setMessages([])
      }
    } catch (err: any) {
      message.error('Query failed: ' + err.message)
    } finally {
      setSearching(false)
    }
  }

  const handleQueryByMsgId = async (values: { topic: string; msgId: string }) => {
    setSearching(true)
    try {
      const data = await fetchApi('/api/message/queryByMsgId', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(values),
      })
      if (data.success) {
        setMessages(data.messages || [])
        setLastQuery({ mode: 'MsgId', total: data.total })
      } else {
        message.error(data.error || 'Message not found')
        setMessages([])
      }
    } catch (err: any) {
      message.error('Query failed: ' + err.message)
    } finally {
      setSearching(false)
    }
  }

  const handleQueryByTime = async (values: { topic: string; range: any; maxNum?: number }) => {
    setSearching(true)
    try {
      const [begin, end] = values.range || []
      const data = await fetchApi('/api/message/queryByTime', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topic: values.topic,
          beginTime: begin ? begin.valueOf() : Date.now() - 24 * 60 * 60 * 1000,
          endTime: end ? end.valueOf() : Date.now(),
          maxNum: values.maxNum || 32,
        }),
      })
      if (data.success) {
        setMessages(data.messages || [])
        setLastQuery({ mode: 'Time Range', total: data.total })
        if ((data.messages || []).length === 0) message.info('No messages found')
      } else {
        message.error(data.error || 'Query failed')
        setMessages([])
      }
    } catch (err: any) {
      message.error('Query failed: ' + err.message)
    } finally {
      setSearching(false)
    }
  }

  const handleClear = () => {
    setMessages([])
    setLastQuery(null)
    formKey.resetFields()
    formMsgId.resetFields()
    formTime.resetFields()
  }

  const columns = [
    { title: '#', key: 'index', width: 40, render: (_: any, __: any, i: number) => i + 1 },
    {
      title: 'MsgId', dataIndex: 'msgId', key: 'msgId', width: 180,
      render: (v: string) => <Text copyable style={{ fontSize: 11 }}>{v}</Text>,
    },
    { title: 'Tags', dataIndex: 'tags', key: 'tags', width: 70, render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: 'Keys', dataIndex: 'keys', key: 'keys', width: 100, render: (v: string) => v || '-' },
    {
      title: 'Body', dataIndex: 'body', key: 'body', ellipsis: true, width: 200,
      render: (v: string) => (
        <Text copyable={{ text: v }} style={{ fontSize: 12, wordBreak: 'break-all' }}>
          {v}
        </Text>
      ),
    },
    { title: 'Queue', key: 'queue', width: 80, render: (_: any, r: MessageRecord) => `Q${r.queueId}:${r.queueOffset}` },
    { title: 'Size', dataIndex: 'msgSize', key: 'msgSize', width: 60, render: (v: number) => `${v}B` },
    {
      title: 'Store Time', key: 'storeTime', width: 160,
      render: (_: any, r: MessageRecord) => {
        const d = new Date(r.storeTimestamp)
        return <Text style={{ fontSize: 11 }}>{d.toLocaleString()}</Text>
      },
    },
    { title: 'Store Host', dataIndex: 'storeHost', key: 'storeHost', width: 130, render: (v: string) => <Text style={{ fontSize: 11 }}>{v || '-'}</Text> },
    { title: 'Retry', dataIndex: 'reconsumeTimes', key: 'reconsumeTimes', width: 50 },
  ]

  const topicSelect = (
    <Select
      showSearch
      placeholder="Select or search a topic"
      loading={loadingTopics}
      options={topics.map(t => ({ value: t, label: t }))}
      filterOption={(input: string, option: any) =>
        (option?.label as string)?.toLowerCase().includes(input.toLowerCase())
      }
    />
  )

  const tabItems = [
    {
      key: 'key' as QueryMode,
      label: <span><KeyOutlined /> By Key</span>,
      children: (
        <Form form={formKey} layout="vertical" onFinish={handleQueryByKey} initialValues={{ maxNum: 32 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="topic" label="Topic" rules={[{ required: true }]}>
                {topicSelect}
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="key" label="Message Key" rules={[{ required: true, message: 'Please input message key' }]}>
                <Input placeholder="Business key / order ID" />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16} style={{ marginTop: 8 }}>
            <Col span={12}>
              <Form.Item name="maxNum" label="Max Results">
                <InputNumber min={1} max={256} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={12} style={{ display: 'flex', alignItems: 'flex-end', paddingBottom: 24 }}>
              <Space>
                <Button type="primary" htmlType="submit" icon={<SearchOutlined />} loading={searching}>
                  Search
                </Button>
                <Button icon={<ClearOutlined />} onClick={handleClear} disabled={searching}>
                  Clear
                </Button>
              </Space>
            </Col>
          </Row>
        </Form>
      ),
    },
    {
      key: 'msgId' as QueryMode,
      label: <span><NumberOutlined /> By MsgId</span>,
      children: (
        <Form form={formMsgId} layout="vertical" onFinish={handleQueryByMsgId}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="topic" label="Topic" rules={[{ required: true }]}>
                {topicSelect}
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="msgId" label="Message ID" rules={[{ required: true, message: 'Please input message ID' }]}>
                <Input placeholder="Exact message ID" />
              </Form.Item>
            </Col>
          </Row>
          <Row style={{ marginTop: 8 }}>
            <Col span={12} style={{ display: 'flex', alignItems: 'flex-end', paddingBottom: 24 }}>
              <Space>
                <Button type="primary" htmlType="submit" icon={<SearchOutlined />} loading={searching}>
                  Search
                </Button>
                <Button icon={<ClearOutlined />} onClick={handleClear} disabled={searching}>
                  Clear
                </Button>
              </Space>
            </Col>
          </Row>
        </Form>
      ),
    },
    {
      key: 'time' as QueryMode,
      label: <span><ClockCircleOutlined /> By Time</span>,
      children: (
        <Form form={formTime} layout="vertical" onFinish={handleQueryByTime} initialValues={{ maxNum: 32 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="topic" label="Topic" rules={[{ required: true }]}>
                {topicSelect}
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="range" label="Time Range">
                <RangePicker showTime style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16} style={{ marginTop: 8 }}>
            <Col span={12}>
              <Form.Item name="maxNum" label="Max Results">
                <InputNumber min={1} max={256} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={12} style={{ display: 'flex', alignItems: 'flex-end', paddingBottom: 24 }}>
              <Space>
                <Button type="primary" htmlType="submit" icon={<SearchOutlined />} loading={searching}>
                  Search
                </Button>
                <Button icon={<ClearOutlined />} onClick={handleClear} disabled={searching}>
                  Clear
                </Button>
              </Space>
            </Col>
          </Row>
        </Form>
      ),
    },
  ]

  return (
    <Row gutter={[24, 24]}>
      <Col span={24}>
        <Card>
          <Tabs activeKey={queryMode} onChange={mode => { setQueryMode(mode as QueryMode); setMessages([]); setLastQuery(null) }} items={tabItems} />
        </Card>
      </Col>

      <Col span={24}>
        <Card
          title={
            lastQuery
              ? `Results (${lastQuery.mode}) — ${lastQuery.total} message${lastQuery.total !== 1 ? 's' : ''}`
              : 'Results'
          }
        >
          <Table
            dataSource={messages}
            columns={columns}
            rowKey={(r: MessageRecord) => r.msgId}
            pagination={false}
            size="small"
            scroll={{ x: 1100, y: 500 }}
            locale={{ emptyText: <Empty description="Click Search to query messages" /> }}
          />
        </Card>
      </Col>
    </Row>
  )
}
