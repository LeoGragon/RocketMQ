import { useCallback, useEffect, useRef, useState } from 'react'
import {
  Card, Form, Input, Select, Button, Row, Col,
  Typography, Space, Tag, InputNumber, Radio, Switch, Divider,
  Statistic, Popconfirm, message, Modal, Drawer, Table, Descriptions, Tooltip,
} from 'antd'
import {
  ThunderboltOutlined, SendOutlined, CustomerServiceOutlined,
  StopOutlined, ReloadOutlined, PlusOutlined, PauseCircleOutlined,
  ExclamationCircleOutlined,
  DeleteOutlined,
} from '@ant-design/icons'

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

const MSG_TYPE_COLORS: Record<string, string> = { NORMAL: 'default', ORDER: 'purple', DELAY: 'orange', TRANSACTION: 'red' }

const isSystemTopic = (topic: string): boolean => {
  if (topic === 'TBW102' || topic === 'BenchmarkTest' || topic === 'SELF_TEST_TOPIC' || topic === 'OFFSET_MOVED_EVENT') return true
  if (topic.startsWith('SCHEDULE_TOPIC_')) return true
  if (topic.startsWith('rmq_sys_') || topic.startsWith('RMQ_SYS_')) return true
  if (topic.includes('DefaultCluster')) return true
  if (topic.startsWith('%')) return true
  return false
}

interface TaskInfo {
  taskId: string
  type: 'PRODUCER' | 'CONSUMER'
  topic: string
  messageType: string
  sentCount: number
  runSentCount: number
  totalMaxCount: number
  runMaxCount: number
  failedCount: number
  consumedCount: number
  running: boolean
  elapsedMs: number
  tps: number
  lastMsgId?: string
  lastError?: string
}

interface MessageRecord {
  id: number
  task_id: string
  direction: string
  topic: string
  tags: string
  msg_keys: string
  body: string
  msg_id: string
  queue_id: number
  queue_offset: number
  message_type: string
  born_timestamp: number
  store_timestamp: number
  created_at: string
}

const PAGE_SIZE = 50

export default function Simulation() {
  const [topics, setTopics] = useState<string[]>([])
  const [loadingTopics, setLoadingTopics] = useState(false)
  const [tasks, setTasks] = useState<TaskInfo[]>([])
  const timerRef = useRef<number | null>(null)

  // Modal state
  const [modalOpen, setModalOpen] = useState<'producer' | 'consumer' | null>(null)
  const [prodForm] = Form.useForm()
  const [consForm] = Form.useForm()
  const [prodType, setProdType] = useState('NORMAL')
  const [txCommit, setTxCommit] = useState(true)
  const [startingProd, setStartingProd] = useState(false)
  const [startingCons, setStartingCons] = useState(false)

  // Drawer state
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedTask, setSelectedTask] = useState<TaskInfo | null>(null)
  const [drawerMessages, setDrawerMessages] = useState<MessageRecord[]>([])
  const [drawerTotal, setDrawerTotal] = useState(0)
  const [drawerPage, setDrawerPage] = useState(0)
  const [drawerLoading, setDrawerLoading] = useState(false)
  const topicOptions = topics.filter(t => !isSystemTopic(t)).map(t => ({ value: t, label: t }))

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

  const loadTasks = async () => {
    try {
      const data = await fetchApi('/api/simulation/tasks')
      setTasks(data || [])
    } catch {
      // ignore
    }
  }

  const loadDrawerMessages = useCallback(async (task: TaskInfo, page: number) => {
    setDrawerLoading(true)
    try {
      const isProducer = task.type === 'PRODUCER'
      const endpoint = isProducer
        ? `/api/simulation/producer/${task.taskId}/messages?offset=${page * PAGE_SIZE}&limit=${PAGE_SIZE}`
        : `/api/simulation/consumer/${task.taskId}/messages?offset=${page * PAGE_SIZE}&limit=${PAGE_SIZE}`
      const data = await fetchApi(endpoint)
      setDrawerMessages(data.messages || [])
      setDrawerTotal(data.total || 0)
    } catch {
      setDrawerMessages([])
      setDrawerTotal(0)
    } finally {
      setDrawerLoading(false)
    }
  }, [])

  const openMessageDrawer = (task: TaskInfo) => {
    setSelectedTask(task)
    setDrawerPage(0)
    setDrawerOpen(true)
    loadDrawerMessages(task, 0)
  }

  useEffect(() => {
    loadTopics()
    loadTasks()
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [])

  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (tasks.length > 0) {
      timerRef.current = window.setInterval(loadTasks, 1000)
    }
  }, [tasks.length])

  const handleStartProducer = async (values: any) => {
    setStartingProd(true)
    try {
      const payload: any = {
        topic: values.topic,
        tags: values.tags || '',
        keys: values.keys || '',
        messageType: prodType,
        intervalMs: values.intervalMs ?? 1000,
        totalMaxCount: values.totalMaxCount ?? 0,
        runMaxCount: values.runMaxCount ?? 0,
        bodyTemplate: values.bodyTemplate || '',
      }
      if (prodType === 'DELAY') payload.delayLevel = values.delayLevel ?? 3
      if (prodType === 'TRANSACTION') payload.transactionCommit = txCommit
      const data = await fetchApi('/api/simulation/producer/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (data.success) {
        message.success(`Producer started: ${data.taskId}`)
        prodForm.resetFields()
        setProdType('NORMAL')
        setTxCommit(true)
        setModalOpen(null)
        loadTasks()
      } else {
        message.error(data.error || 'Failed to start producer')
      }
    } catch (err: any) {
      message.error('Failed: ' + err.message)
    } finally {
      setStartingProd(false)
    }
  }

  const handleStartConsumer = async (values: any) => {
    setStartingCons(true)
    try {
      const payload = {
        topic: values.topic,
        consumerGroup: values.consumerGroup,
        tagFilter: values.tagFilter || '',
        pollIntervalMs: values.pollIntervalMs ?? 1000,
      }
      const data = await fetchApi('/api/simulation/consumer/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (data.success) {
        message.success(`Consumer started: ${data.taskId}`)
        consForm.resetFields()
        setModalOpen(null)
        loadTasks()
      } else {
        message.error(data.error || 'Failed to start consumer')
      }
    } catch (err: any) {
      message.error('Failed: ' + err.message)
    } finally {
      setStartingCons(false)
    }
  }

  const handleStop = async (taskId: string) => {
    const task = tasks.find(t => t.taskId === taskId)
    const isProducer = task?.type === 'PRODUCER'
    const url = isProducer
      ? '/api/simulation/producer/stop'
      : '/api/simulation/consumer/stop'
    try {
      await fetchApi(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ taskId }),
      })
      loadTasks()
    } catch (err: any) {
      message.error('Failed to stop: ' + err.message)
    }
  }

  const handleStopAll = async () => {
    try {
      const data = await fetchApi('/api/simulation/stop-all', { method: 'POST' })
      message.success(`Stopped ${data.stopped} tasks`)
      loadTasks()
    } catch (err: any) {
      message.error('Failed: ' + err.message)
    }
  }

  const handleRestart = async (task: TaskInfo) => {
    const isProducer = task.type === 'PRODUCER'
    const url = isProducer
      ? '/api/simulation/producer/restart'
      : '/api/simulation/consumer/restart'
    try {
      const data = await fetchApi(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ taskId: task.taskId }),
      })
      if (data.success) {
        message.success(`${isProducer ? 'Producer' : 'Consumer'} restarted: ${data.taskId}`)
        loadTasks()
      } else {
        message.error(data.error || 'Failed to restart task')
      }
    } catch (err: any) {
      message.error('Failed to restart: ' + err.message)
    }
  }

  const handleRemove = async (taskId: string) => {
    try {
      await fetchApi('/api/simulation/task/remove', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ taskId }),
      })
      message.success(`Removed: ${taskId}`)
      loadTasks()
    } catch (err: any) {
      message.error('Failed: ' + err.message)
    }
  }

  const formatElapsed = (ms: number) => {
    const s = Math.floor(ms / 1000)
    if (s < 60) return `${s}s`
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
    const h = Math.floor(s / 3600)
    return `${h}h ${Math.floor((s % 3600) / 60)}m`
  }

  const openProducerModal = () => {
    prodForm.resetFields()
    setProdType('NORMAL')
    setTxCommit(true)
    setModalOpen('producer')
  }

  const openConsumerModal = () => {
    consForm.resetFields()
    setModalOpen('consumer')
  }

  const activeTasks = tasks.filter(t => t.running)

  // ── Card renderer ──────────────────────────────────────────────

  const renderTaskCard = (task: TaskInfo) => {
    const isProducer = task.type === 'PRODUCER'
    const statusColor = task.running ? '#52c41a' : '#d9d9d9'
    const typeColor = isProducer ? 'blue' : 'green'
    const typeIcon = isProducer ? <SendOutlined /> : <CustomerServiceOutlined />
    const renderMetric = (label: string, value: number | string, color?: string) => (
      <div style={{ minWidth: 0 }}>
        <div style={{ fontSize: 11, color: '#8c8c8c', lineHeight: '16px' }}>{label}</div>
        <div style={{
          color: color || '#262626',
          fontSize: 18,
          lineHeight: '24px',
          fontWeight: 600,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}>
          {value}
        </div>
      </div>
    )

    const card = (
      <Card
        size="small"
        hoverable
        bodyStyle={{ padding: 14 }}
        style={{
          borderLeft: `3px solid ${statusColor}`,
          opacity: task.running ? 1 : 0.65,
          cursor: 'pointer',
          width: '100%',
          height: '100%',
        }}
        onClick={() => openMessageDrawer(task)}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10 }}>
            <Space size={6} style={{ minWidth: 0 }}>
              <span style={{
                width: 8, height: 8, borderRadius: '50%', display: 'inline-block',
                flexShrink: 0,
                backgroundColor: statusColor, boxShadow: task.running ? `0 0 6px ${statusColor}` : 'none',
              }} />
              <Tag color={typeColor} icon={typeIcon} style={{ marginInlineEnd: 0 }}>
                {isProducer ? 'Producer' : 'Consumer'}
              </Tag>
              <Tag color={task.running ? 'success' : 'default'} style={{ marginInlineEnd: 0 }}>
                {task.running ? 'Running' : 'Stopped'}
              </Tag>
            </Space>

            <Space size={4} style={{ flexShrink: 0 }} onClick={e => e.stopPropagation()}>
              {task.running ? (
                <Popconfirm title="Stop this task?" onConfirm={() => handleStop(task.taskId)}>
                  <Tooltip title="Stop">
                    <Button size="small" danger icon={<PauseCircleOutlined />} />
                  </Tooltip>
                </Popconfirm>
              ) : (
                <>
                  <Popconfirm title="Restart this task with the same config?" onConfirm={() => handleRestart(task)}>
                    <Tooltip title="Restart">
                      <Button size="small" type="primary" icon={<ReloadOutlined />} />
                    </Tooltip>
                  </Popconfirm>
                  <Popconfirm title="Remove this task?" onConfirm={() => handleRemove(task.taskId)}>
                    <Tooltip title="Remove">
                      <Button size="small" icon={<DeleteOutlined />} />
                    </Tooltip>
                  </Popconfirm>
                </>
              )}
            </Space>
          </div>

          <div style={{ minWidth: 0 }}>
            <Text strong style={{ display: 'block', fontSize: 17, lineHeight: '24px' }} ellipsis={{ tooltip: task.topic }}>
              {task.topic}
            </Text>
            {isProducer && (
              <div style={{ marginTop: 4 }}>
                <Tag color={MSG_TYPE_COLORS[task.messageType] || 'default'} style={{ marginInlineEnd: 0 }}>
                  {task.messageType}
                </Tag>
              </div>
            )}
          </div>

          <div style={{
            display: 'grid',
            gridTemplateColumns: isProducer ? 'repeat(4, minmax(0, 1fr))' : 'repeat(2, minmax(0, 1fr))',
            gap: 12,
          }}>
            {isProducer ? (
              <>
                {renderMetric('Total Sent', task.sentCount, '#1677ff')}
                {renderMetric('This Run', task.runSentCount ?? 0, '#13c2c2')}
                {renderMetric('Failed', task.failedCount, task.failedCount > 0 ? '#ff4d4f' : '#8c8c8c')}
                {renderMetric('TPS', Math.round(task.tps * 10) / 10)}
              </>
            ) : (
              <>
                {renderMetric('Consumed', task.consumedCount, '#52c41a')}
                {renderMetric('Runtime', formatElapsed(task.elapsedMs))}
              </>
            )}
          </div>

          {task.lastError && (
            <Text type="danger" style={{ fontSize: 12 }} ellipsis={{ tooltip: task.lastError }}>
              <ExclamationCircleOutlined /> {task.lastError}
            </Text>
          )}
        </div>
      </Card>
    )

    return (
      <Col xs={24} sm={12} md={8} xxl={6} key={task.taskId} style={{ display: 'flex' }}>
        {card}
      </Col>
    )
  }

  // ── Message table columns ─────────────────────────────────────

  const msgColumns = [
    { title: '#', width: 50, render: (_: any, __: any, i: number) => drawerPage * PAGE_SIZE + i + 1 },
    { title: 'MsgId', dataIndex: 'msg_id', width: 180,
      render: (v: string) => <Text copyable style={{ fontSize: 12 }}>{v}</Text> },
    { title: 'Tags', dataIndex: 'tags', width: 70,
      render: (v: string) => v ? <Tag color="blue">{v}</Tag> : '-' },
    { title: 'Keys', dataIndex: 'msg_keys', width: 100,
      render: (v: string) => v || '-' },
    { title: 'Body', dataIndex: 'body', ellipsis: true,
      render: (v: string) => <Text style={{ fontSize: 12, wordBreak: 'break-all' }}>{v}</Text> },
    { title: 'Queue', width: 80,
      render: (_: any, r: MessageRecord) => <Text code>Q{r.queue_id}:{r.queue_offset}</Text> },
    { title: 'Type', dataIndex: 'message_type', width: 90,
      render: (v: string) => v ? <Tag color={MSG_TYPE_COLORS[v] || 'default'}>{v}</Tag> : '-' },
    { title: 'Time', dataIndex: 'created_at', width: 150,
      render: (v: string) => v ? new Date(v).toLocaleString() : '-' },
  ]

  const consMsgColumns = [
    { title: '#', width: 50, render: (_: any, __: any, i: number) => drawerPage * PAGE_SIZE + i + 1 },
    { title: 'MsgId', dataIndex: 'msg_id', width: 180,
      render: (v: string) => <Text copyable style={{ fontSize: 12 }}>{v}</Text> },
    { title: 'Tags', dataIndex: 'tags', width: 70,
      render: (v: string) => v ? <Tag color="blue">{v}</Tag> : '-' },
    { title: 'Keys', dataIndex: 'msg_keys', width: 100,
      render: (v: string) => v || '-' },
    { title: 'Body', dataIndex: 'body', ellipsis: true,
      render: (v: string) => <Text style={{ fontSize: 12, wordBreak: 'break-all' }}>{v}</Text> },
    { title: 'Queue', width: 80,
      render: (_: any, r: MessageRecord) => <Text code>Q{r.queue_id}:{r.queue_offset}</Text> },
    { title: 'Born', dataIndex: 'born_timestamp', width: 150,
      render: (v: number) => v ? new Date(v).toLocaleString() : '-' },
    { title: 'Consumed At', dataIndex: 'created_at', width: 150,
      render: (v: string) => v ? new Date(v).toLocaleString() : '-' },
  ]

  // ── Modal content ──────────────────────────────────────────────

  const producerModal = (
    <Modal
      title={<Space><SendOutlined />Start Producer</Space>}
      open={modalOpen === 'producer'}
      onCancel={() => setModalOpen(null)}
      footer={null}
      width={560}
      destroyOnClose
    >
      <Form form={prodForm} layout="vertical" onFinish={handleStartProducer}
        initialValues={{ intervalMs: 1000, totalMaxCount: 0, runMaxCount: 0 }}>
        <Form.Item name="topic" label="Topic" rules={[{ required: true, message: 'Select topic' }]}>
          <Select showSearch placeholder="Select a topic" loading={loadingTopics}
            options={topicOptions}
            filterOption={(input, option) => (option?.label as string)?.toLowerCase().includes(input.toLowerCase())} />
        </Form.Item>
        <Row gutter={12}>
          <Col span={12}>
            <Form.Item name="tags" label="Tag"><Input placeholder="e.g. TagA" /></Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item name="keys" label="Key"><Input placeholder="Message key" /></Form.Item>
          </Col>
        </Row>
        <Form.Item name="bodyTemplate" label="Body Template">
          <Input.TextArea rows={2} placeholder='Use {{seq}} and {{time}} placeholders, or leave empty for auto-generate' />
        </Form.Item>

        <Divider orientation="left" plain>Message Type</Divider>
        <Form.Item>
          <Radio.Group value={prodType} onChange={e => setProdType(e.target.value)}
            buttonStyle="solid" optionType="button">
            <Radio.Button value="NORMAL">Normal</Radio.Button>
            <Radio.Button value="ORDER">Order</Radio.Button>
            <Radio.Button value="DELAY">Delay</Radio.Button>
            <Radio.Button value="TRANSACTION">Transaction</Radio.Button>
          </Radio.Group>
        </Form.Item>

        {prodType === 'DELAY' && (
          <Form.Item name="delayLevel" label="Delay Level">
            <Select options={DELAY_LEVELS} placeholder="Select delay" />
          </Form.Item>
        )}
        {prodType === 'TRANSACTION' && (
          <Form.Item label="Tx Result">
            <Switch checked={txCommit} onChange={setTxCommit}
              checkedChildren="COMMIT" unCheckedChildren="ROLLBACK" />
          </Form.Item>
        )}

        <Divider orientation="left" plain>Send Rate</Divider>
        <Row gutter={12}>
          <Col span={8}>
            <Form.Item name="intervalMs" label="Interval (ms)">
              <InputNumber min={100} max={60000} step={100} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item name="totalMaxCount" label="Total Count (0=∞)">
              <InputNumber min={0} max={999999999} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item name="runMaxCount" label="This Run Count (0=∞)">
              <InputNumber min={0} max={999999} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
        </Row>

        <Form.Item style={{ marginBottom: 0 }}>
          <Button type="primary" htmlType="submit" icon={<SendOutlined />} loading={startingProd} block>
            Start Producer
          </Button>
        </Form.Item>
      </Form>
    </Modal>
  )

  const consumerModal = (
    <Modal
      title={<Space><CustomerServiceOutlined />Start Consumer</Space>}
      open={modalOpen === 'consumer'}
      onCancel={() => setModalOpen(null)}
      footer={null}
      width={500}
      destroyOnClose
    >
      <Form form={consForm} layout="vertical" onFinish={handleStartConsumer}
        initialValues={{ pollIntervalMs: 1000 }}>
        <Form.Item name="topic" label="Topic" rules={[{ required: true, message: 'Select topic' }]}>
          <Select showSearch placeholder="Select a topic" loading={loadingTopics}
            options={topicOptions}
            filterOption={(input, option) => (option?.label as string)?.toLowerCase().includes(input.toLowerCase())} />
        </Form.Item>
        <Form.Item name="consumerGroup" label="Consumer Group" rules={[{ required: true, message: 'Input consumer group' }]}>
          <Input placeholder="e.g. sim-consumer-group" />
        </Form.Item>
        <Form.Item name="tagFilter" label="Tag Filter">
          <Input placeholder="e.g. TagA || TagB (default: *)" />
        </Form.Item>
        <Form.Item name="pollIntervalMs" label="Poll Interval (ms)">
          <InputNumber min={200} max={30000} step={200} style={{ width: '100%' }} />
        </Form.Item>
        <Form.Item style={{ marginBottom: 0 }}>
          <Button type="primary" htmlType="submit" icon={<CustomerServiceOutlined />} loading={startingCons} block>
            Start Consumer
          </Button>
        </Form.Item>
      </Form>
    </Modal>
  )

  // ── Main render ────────────────────────────────────────────────

  return (
    <>
      {/* Action bar */}
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={[16, 12]} align="middle">
          <Col xs={24} md={10} lg={12}>
            <Space size="middle" wrap>
              <Button type="primary" icon={<PlusOutlined />} onClick={openProducerModal}>
                Start Producer
              </Button>
              <Button icon={<PlusOutlined />} onClick={openConsumerModal}>
                Start Consumer
              </Button>
              <Button icon={<ReloadOutlined />} onClick={loadTopics}>Refresh Topics</Button>
            </Space>
          </Col>
          <Col xs={24} md={14} lg={12}>
            <Space size="middle" wrap>
              <Statistic title="Producers" value={activeTasks.filter(t => t.type === 'PRODUCER').length}
                prefix={<SendOutlined />} valueStyle={{ fontSize: 16 }} />
              <Statistic title="Consumers" value={activeTasks.filter(t => t.type === 'CONSUMER').length}
                prefix={<CustomerServiceOutlined />} valueStyle={{ fontSize: 16 }} />
              <Statistic title="Total Sent"
                value={activeTasks.reduce((s, t) => s + t.sentCount, 0)}
                prefix={<ThunderboltOutlined />} valueStyle={{ fontSize: 16, color: '#1677ff' }} />
              <Statistic title="Total Consumed"
                value={activeTasks.reduce((s, t) => s + t.consumedCount, 0)}
                valueStyle={{ fontSize: 16, color: '#52c41a' }} />
              <Popconfirm title="Stop ALL running tasks?" onConfirm={handleStopAll}
                disabled={activeTasks.length === 0}
                okText="Confirm" cancelText="Cancel">
                <Button danger icon={<StopOutlined />} disabled={activeTasks.length === 0}>
                  Stop All
                </Button>
              </Popconfirm>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Task cards */}
      {tasks.length === 0 ? (
        <Card>
          <div style={{ textAlign: 'center', padding: 48, color: '#999' }}>
            <ThunderboltOutlined style={{ fontSize: 48, marginBottom: 16 }} />
            <div style={{ fontSize: 16 }}>No active simulation tasks</div>
            <div style={{ fontSize: 13, marginTop: 8 }}>
              Click <Text strong>"Start Producer"</Text> or <Text strong>"Start Consumer"</Text> above to begin
            </div>
          </div>
        </Card>
      ) : (
        <Row gutter={[16, 16]}>
          {tasks.map(renderTaskCard)}
        </Row>
      )}

      {/* Message drawer */}
      <Drawer
        title={selectedTask ? (
          <Space>
            {selectedTask.type === 'PRODUCER' ? <SendOutlined /> : <CustomerServiceOutlined />}
            <span>{selectedTask.taskId}</span>
            <Tag color={selectedTask.running ? 'green' : 'default'}>
              {selectedTask.running ? 'Running' : 'Stopped'}
            </Tag>
            <Tag color={selectedTask.type === 'PRODUCER' ? 'blue' : 'green'}>
              {selectedTask.type === 'PRODUCER' ? 'Producer' : 'Consumer'}
            </Tag>
          </Space>
        ) : 'Messages'}
        open={drawerOpen}
        onClose={() => { setDrawerOpen(false); setSelectedTask(null) }}
        width={900}
      >
        {selectedTask && (
          <>
            <Descriptions size="small" column={3} bordered style={{ marginBottom: 16 }}>
              <Descriptions.Item label="Topic">{selectedTask.topic}</Descriptions.Item>
              <Descriptions.Item label="Type">
                <Tag color={selectedTask.type === 'PRODUCER' ? 'blue' : 'green'}>
                  {selectedTask.type}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Status">
                <Tag color={selectedTask.running ? 'green' : 'default'}>
                  {selectedTask.running ? 'Running' : 'Stopped'}
                </Tag>
              </Descriptions.Item>
              {selectedTask.type === 'PRODUCER' ? (
                <>
                  <Descriptions.Item label="Total Sent">{selectedTask.sentCount.toLocaleString()}</Descriptions.Item>
                  <Descriptions.Item label="This Run">{(selectedTask.runSentCount ?? 0).toLocaleString()}</Descriptions.Item>
                  <Descriptions.Item label="Failed">
                    <Text type={selectedTask.failedCount > 0 ? 'danger' : 'secondary'}>
                      {selectedTask.failedCount}
                    </Text>
                  </Descriptions.Item>
                  <Descriptions.Item label="TPS">{selectedTask.tps.toFixed(1)}</Descriptions.Item>
                  <Descriptions.Item label="Total Limit">
                    {selectedTask.totalMaxCount > 0 ? selectedTask.totalMaxCount.toLocaleString() : '∞'}
                  </Descriptions.Item>
                  <Descriptions.Item label="This Run Limit">
                    {selectedTask.runMaxCount > 0 ? selectedTask.runMaxCount.toLocaleString() : '∞'}
                  </Descriptions.Item>
                </>
              ) : (
                <>
                  <Descriptions.Item label="Consumed">{selectedTask.consumedCount.toLocaleString()}</Descriptions.Item>
                  <Descriptions.Item label="Runtime">{formatElapsed(selectedTask.elapsedMs)}</Descriptions.Item>
                  <Descriptions.Item label="TPS">{selectedTask.tps.toFixed(1)}</Descriptions.Item>
                </>
              )}
            </Descriptions>

            <Table
              dataSource={drawerMessages}
              columns={selectedTask.type === 'PRODUCER' ? msgColumns : consMsgColumns}
              rowKey="id"
              size="small"
              loading={drawerLoading}
              scroll={{ y: 500 }}
              pagination={{
                current: drawerPage + 1,
                pageSize: PAGE_SIZE,
                total: drawerTotal,
                showTotal: (t: number) => `${t} messages`,
                showSizeChanger: false,
                onChange: (page: number) => {
                  const p = page - 1
                  setDrawerPage(p)
                  if (selectedTask) loadDrawerMessages(selectedTask, p)
                },
              }}
              locale={{ emptyText: 'No messages' }}
            />
          </>
        )}
      </Drawer>

      {producerModal}
      {consumerModal}
    </>
  )
}
