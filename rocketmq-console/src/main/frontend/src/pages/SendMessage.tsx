import { useEffect, useState } from 'react'
import {
  Card, Form, Input, Select, Button, Radio,
  message, Row, Col, Space, Divider, Typography, Switch, Alert,
} from 'antd'
import { SendOutlined, ClearOutlined } from '@ant-design/icons'

const { TextArea } = Input
const { Text } = Typography

const DELAY_LEVELS: { value: number; label: string }[] = [
  { value: 1, label: '1s' }, { value: 2, label: '5s' },
  { value: 3, label: '10s' }, { value: 4, label: '30s' },
  { value: 5, label: '1m' }, { value: 6, label: '2m' },
  { value: 7, label: '3m' }, { value: 8, label: '4m' },
  { value: 9, label: '5m' }, { value: 10, label: '6m' },
  { value: 11, label: '7m' }, { value: 12, label: '8m' },
  { value: 13, label: '9m' }, { value: 14, label: '10m' },
  { value: 15, label: '20m' }, { value: 16, label: '30m' },
  { value: 17, label: '1h' }, { value: 18, label: '2h' },
]

export default function SendMessage() {
  const [form] = Form.useForm()
  const [sending, setSending] = useState(false)
  const [topics, setTopics] = useState<string[]>([])
  const [loadingTopics, setLoadingTopics] = useState(false)
  const [messageType, setMessageType] = useState('NORMAL')

  const isSystemTopic = (topic: string): boolean => {
    if (topic === 'TBW102' || topic === 'BenchmarkTest' || topic === 'SELF_TEST_TOPIC' || topic === 'OFFSET_MOVED_EVENT') return true
    if (topic.startsWith('SCHEDULE_TOPIC_')) return true
    if (topic.startsWith('rmq_sys_') || topic.startsWith('RMQ_SYS_')) return true
    if (topic.includes('DefaultCluster')) return true
    if (topic.startsWith('%')) return true
    return false
  }

  const [transactionCommit, setTransactionCommit] = useState(true)

  const fetchApi = async (path: string, opts?: RequestInit) => {
    const res = await fetch(path, opts)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  }

  const loadTopics = () => {
    setLoadingTopics(true)
    fetchApi('/api/topics')
      .then(data => setTopics(data.topics || []))
      .catch(() => message.error('Failed to load topics'))
      .finally(() => setLoadingTopics(false))
  }

  useEffect(() => { loadTopics() }, [])

  const handleSend = async (values: any) => {
    setSending(true)
    try {
      const payload: any = {
        topic: values.topic,
        tags: values.tags || '',
        keys: values.keys || '',
        body: values.body || '',
        messageType: messageType,
      }
      if (messageType === 'DELAY') {
        payload.delayLevel = values.delayLevel || 3
      }
      if (messageType === 'TRANSACTION') {
        payload.transactionCommit = transactionCommit
      }
      const data = await fetchApi('/api/message/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (data.success) {
        message.success(`Sent! msgId: ${data.msgId}, queueId: ${data.queueId}`)
      } else {
        message.error(data.error || 'Failed to send message')
      }
    } catch (err: any) {
      message.error('Failed to send message: ' + err.message)
    } finally {
      setSending(false)
    }
  }

  const handleReset = () => {
    form.resetFields()
    setMessageType('NORMAL')
    setTransactionCommit(true)
  }

  return (
    <Row gutter={[24, 24]}>
      <Col xs={24} lg={14}>
        <Card title="Send Message">
          <Form
            form={form}
            layout="vertical"
            onFinish={handleSend}
            initialValues={{ delayLevel: 3 }}
          >
            <Form.Item name="topic" label="Topic" rules={[{ required: true, message: 'Please select a topic' }]}>
              <Select
                showSearch
                placeholder="Select or search a topic"
                loading={loadingTopics}
                options={topics.filter(t => !isSystemTopic(t)).map(t => ({ value: t, label: t }))}
                filterOption={(input, option) =>
                  (option?.label as string)?.toLowerCase().includes(input.toLowerCase())
                }
              />
            </Form.Item>

            <Row gutter={16}>
              <Col span={12}>
                <Form.Item name="tags" label="Tag">
                  <Input placeholder="e.g. TagA (default: *)" />
                </Form.Item>
              </Col>
              <Col span={12}>
                <Form.Item name="keys" label="Key">
                  <Input placeholder="Message key (business ID)" />
                </Form.Item>
              </Col>
            </Row>

            <Form.Item name="body" label="Message Body" rules={[{ required: true, message: 'Please input message body' }]}>
              <TextArea rows={6} placeholder="Message body content (JSON or plain text)" />
            </Form.Item>

            <Divider orientation="left" plain>Message Type</Divider>

            <Form.Item>
              <Radio.Group
                value={messageType}
                onChange={e => setMessageType(e.target.value)}
                buttonStyle="solid"
                optionType="button"
              >
                <Radio.Button value="NORMAL">Normal</Radio.Button>
                <Radio.Button value="ORDER">Order (FIFO)</Radio.Button>
                <Radio.Button value="DELAY">Delay</Radio.Button>
                <Radio.Button value="TRANSACTION">Transaction</Radio.Button>
              </Radio.Group>
            </Form.Item>

            {messageType === 'ORDER' && (
              <Alert
                message="Messages with the same Key will be sent to the same queue, ensuring FIFO order by Key."
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />
            )}

            {messageType === 'DELAY' && (
              <Form.Item name="delayLevel" label="Delay Level">
                <Select
                  placeholder="Select delay level"
                  options={DELAY_LEVELS}
                />
              </Form.Item>
            )}

            {messageType === 'TRANSACTION' && (
              <Form.Item label="Transaction Result">
                <Space>
                  <Text>Transaction commit:</Text>
                  <Switch
                    checked={transactionCommit}
                    onChange={setTransactionCommit}
                    checkedChildren="COMMIT"
                    unCheckedChildren="ROLLBACK"
                  />
                </Space>
              </Form.Item>
            )}

            <Form.Item>
              <Space>
                <Button type="primary" htmlType="submit" icon={<SendOutlined />} loading={sending}>
                  Send Message
                </Button>
                <Button icon={<ClearOutlined />} onClick={handleReset} disabled={sending}>
                  Reset
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </Card>
      </Col>

      <Col xs={24} lg={10}>
        <Card title="帮助">
          <div style={{ lineHeight: 2 }}>
            <h4>消息类型</h4>
            <p><strong>普通消息:</strong> 标准的无序消息。每条消息独立地分布在队列中。</p>
            <p><strong>顺序消息 (FIFO):</strong> 具有相同 Key 的消息会被路由到同一个队列，保证按 Key 严格有序。</p>
            <p><strong>延时消息:</strong> 消息将在指定的延迟时间（1秒 ~ 2小时，共18个级别）后投递。</p>
            <p><strong>事务消息:</strong> 可以提交或回滚的半消息，适用于分布式事务场景。</p>

            <h4 style={{ marginTop: 16 }}>延迟级别</h4>
            <p>1: 1s · 2: 5s · 3: 10s · 4: 30s · 5: 1m · 6: 2m<br/>
            7: 3m · 8: 4m · 9: 5m · 10: 6m · 11: 7m · 12: 8m<br/>
            13: 9m · 14: 10m · 15: 20m · 16: 30m · 17: 1h · 18: 2h</p>

            <h4 style={{ marginTop: 16 }}>标签与键</h4>
            <p><strong>Tag:</strong> 用于更细粒度的消息过滤，消费者可以按 Tag 订阅。</p>
            <p><strong>Key:</strong> 业务标识符，用于消息查找和排序。</p>
          </div>
        </Card>
      </Col>
    </Row>
  )
}
