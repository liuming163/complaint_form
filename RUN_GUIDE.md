# 我自己平时怎么启动这个项目

这是我自己看的启动说明，尽量写简单一点。

## 先记住一个最重要的提醒

**不要同时开两个 `worker.py`。**

现在这个项目的规则是：
- 同一时间只允许一个 worker 真正执行任务
- 队列里的任务按顺序一个一个跑
- 如果误开第二个 `worker.py`，它会直接退出

所以平时正常开发时：
- `app.py` 只开一个
- `worker.py` 只开一个

---

## 一、先记住现在项目需要什么

现在这个项目运行要靠 3 个东西：

1. Redis
2. Web 服务：`app.py`
3. Worker：`worker.py`

其中：
- `app.py` 负责页面、提交表单、查状态
- `worker.py` 负责真正执行投诉任务
- Redis 负责排队

---

## 二、我平时推荐的启动方式

### 第 1 步：先确认 Redis 有没有启动

先执行：

```bash
redis-cli ping
```

如果返回：

```text
PONG
```

说明 Redis 已经在运行了，可以直接继续下面的步骤。

如果提示连不上 Redis，再启动 Redis。

---

### 第 2 步：如果 Redis 没启动，就把它启动

#### 方式 A：后台启动（推荐）
如果是我平时自己开发，优先用这个：

```bash
brew services start redis
```

这样 Redis 会在后台运行，不需要一直单独开一个终端挂着。

启动后再检查一次：

```bash
redis-cli ping
```

看到 `PONG` 就行。

#### 方式 B：前台启动
如果不用后台服务，也可以直接运行：

```bash
redis-server
```

这种方式要一直开着这个终端，关掉就停了。

---

## 三、启动项目

如果 Redis 已经起来了，我只需要再开 **2 个终端**。

### 终端 1：启动 Web
在项目目录下运行：

```bash
python3 app.py
```

启动后浏览器访问：

```text
http://127.0.0.1:5001
```

---

### 终端 2：启动 Worker
在项目目录下运行：

```bash
python3 worker.py
```

这个终端不要关，它负责消费 Redis 队列里的任务。

---

## 四、我日常最常用的启动顺序

如果是正常开发，我就按这个顺序来：

### 1. 先检查 Redis
```bash
redis-cli ping
```

### 2. 如果没启动，就启动 Redis
```bash
brew services start redis
```

### 3. 开第一个终端跑 Web
```bash
python3 app.py
```

### 4. 开第二个终端跑 Worker
```bash
python3 worker.py
```

### 5. 打开页面
```text
http://127.0.0.1:5001
```

---

## 五、现在到底需要开几个终端

### 情况 1：Redis 用后台启动（推荐）
比如用了：

```bash
brew services start redis
```

那我只需要开 **2 个终端**：

#### 终端 1
```bash
python3 app.py
```

#### 终端 2
```bash
python3 worker.py
```

### 情况 2：Redis 用前台启动
比如用了：

```bash
redis-server
```

那我需要开 **3 个终端**：

#### 终端 1
```bash
redis-server
```

#### 终端 2
```bash
python3 app.py
```

#### 终端 3
```bash
python3 worker.py
```

---

## 六、怎么停掉

### 停 Web / Worker
如果是在终端前台跑的，直接按：

```text
Ctrl + C
```

### 停 Redis

#### 如果 Redis 是前台启动的
直接在 Redis 那个终端按：

```text
Ctrl + C
```

#### 如果 Redis 是后台启动的
运行：

```bash
brew services stop redis
```

---

## 七、我以后排查时先看什么

### 1. 页面打不开
先看 `app.py` 那个终端有没有报错。
如果显示端口占用，执行： 
```text
lsof -ti:5001 | xargs kill -9
```

再确认浏览器访问的是：

```text
http://127.0.0.1:5001
```

---

### 2. 提交后任务不跑
先看这两个地方：

#### Redis 是否正常
```bash
redis-cli ping
```

#### Worker 终端有没有报错
看 `worker.py` 那个终端输出。

---

### 3. 提交了但是状态一直不变
优先看：
- `worker.py` 终端有没有报错
- Redis 有没有起来
- MySQL 是否能连上

---

### 4. 想看任务为什么成功 / 为什么失败

现在每条任务都会保存两种结果文件，在：

```text
task_results/
```

#### 结果摘要
看：

```text
uc_<submission_id>.json
```

这里能看到：
- `status`
- `complaint_number`
- `complaint_numbers`
- `completed_at`
- `error`

#### 详细运行日志
看：

```text
uc_<submission_id>.log
```

这里能看到：
- 执行命令
- 开始时间
- 子脚本完整 stdout
- 子脚本完整 stderr
- 是否解析到 JSON_RESULT
- 为什么失败 / 为什么超时

如果只是想快速判断任务结果，看 `.json`。

如果要排查细节，看 `.log`。

---

## 八、如果 worker 抢不到锁怎么办

现在项目限制：

- 同一时间只能有一个 `worker.py` 在真正执行任务
- 如果我误开了第二个 `worker.py`，它会直接退出

如果 `worker.py` 终端里看到类似：

```text
Another Redis worker is already running. Exiting.
```

说明现在已经有一个 worker 在跑，或者 Redis 里还残留了旧的 worker 锁。

### 第一步：先确认是不是真的还有 worker 在跑

可以先看自己是不是已经开过一个 `worker.py` 终端没关。

如果已经开着，就不要再开第二个。

### 第二步：如果确认没有 worker 在跑，再检查 Redis 锁

执行：

```bash
redis-cli get uc_complaint_worker_lock
```

#### 情况 1：返回空
说明没有锁，可以直接重新启动：

```bash
python3 worker.py
```

#### 情况 2：返回一串值
说明 Redis 里还留着旧锁。

这时候先手动删掉：

```bash
redis-cli del uc_complaint_worker_lock
```

然后再启动：

```bash
python3 worker.py
```

### 第三步：如果 worker 被强制关掉了

现在锁有自动过期时间，正常情况下就算没手动删，也会在十几秒后自动释放。

如果你不想等，直接执行：

```bash
redis-cli del uc_complaint_worker_lock
```

就行。

---

## 九、现在任务是怎么执行的

当前是：

- Redis 队列排队
- Worker 单进程执行
- 一次只跑一个任务

也就是说：

如果同时来了很多任务，还是会按顺序一个一个执行，不会同时跑很多个。

这个对现在这个项目反而更稳。

---

## 九、我自己最短版本的记忆

如果我只想记最少内容，就记这 3 句：

### 1. 先确认 Redis
```bash
redis-cli ping
```

### 2. 启 Web
```bash
python3 app.py
```

### 3. 启 Worker
```bash
python3 worker.py
```
