CREATE TABLE IF NOT EXISTS sim_producer (
    task_id VARCHAR(16) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    tags VARCHAR(255),
    msg_keys VARCHAR(255),
    message_type VARCHAR(32) NOT NULL DEFAULT 'NORMAL',
    delay_level INT,
    transaction_commit BOOLEAN,
    interval_ms INT NOT NULL DEFAULT 1000,
    total_max_count BIGINT NOT NULL DEFAULT 0,
    max_count BIGINT NOT NULL DEFAULT 0,
    body_template TEXT,
    status VARCHAR(16) NOT NULL DEFAULT 'RUNNING',
    sent_count BIGINT NOT NULL DEFAULT 0,
    last_run_sent_count BIGINT NOT NULL DEFAULT 0,
    failed_count BIGINT NOT NULL DEFAULT 0,
    last_msg_id VARCHAR(64),
    last_error TEXT,
    start_time DATETIME,
    stop_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status)
);

CREATE TABLE IF NOT EXISTS sim_consumer (
    task_id VARCHAR(16) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    tag_filter VARCHAR(255),
    poll_interval_ms INT NOT NULL DEFAULT 1000,
    status VARCHAR(16) NOT NULL DEFAULT 'RUNNING',
    consumed_count BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    start_time DATETIME,
    stop_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status)
);

CREATE TABLE IF NOT EXISTS sim_message (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(16) NOT NULL,
    direction VARCHAR(8) NOT NULL COMMENT 'SEND or CONSUME',
    topic VARCHAR(255) NOT NULL,
    tags VARCHAR(255),
    msg_keys VARCHAR(255),
    body TEXT,
    msg_id VARCHAR(64),
    queue_id INT,
    queue_offset BIGINT,
    message_type VARCHAR(32),
    born_timestamp BIGINT,
    store_timestamp BIGINT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task_id (task_id),
    INDEX idx_direction (direction),
    INDEX idx_created_at (created_at)
);
