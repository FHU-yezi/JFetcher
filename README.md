# 部署

## 环境

- Python 3.9+
- PostgreSQL
- Prefect 2

## 数据库准备

创建用户：

```sql
CREATE ROLE jfetcher LOGIN PASSWORD 'jfetcher';
```

创建数据库：

```sql
CREATE DATABASE jianshu WITH OWNER = jfetcher;
CREATE DATABASE jpep WITH OWNER = jfetcher;
CREATE DATABASE logs;
```

## 配置

复制 `config.example.toml` 文件，将其重命名为 `config.toml`。

```shell
cp config.example.toml config.toml
```

如果您使用 Docker 进行部署：

- jianshu_postgres.host 填写 `postgres`
- jpep_postgres.host 填写 `postgres`
- logging.host 填写 `postgres`
- notify.host 填写 `gotify`

同时，您需要填写正确的 `postgres.user` 和 `postgres.password`。

## 使用 Docker 部署

创建 Docker 网络：

```shell
docker network create gotify
docker network create postgres
docker network create prefect
```

您需要在 `gotify` 网络的 `27017` 端口上运行一个 Gotify 服务。

您需要在 `postgres` 网络的 `5173` 端口上运行一个 PostgreSQL 服务，身份验证相关信息请参考 `部署 - 数据库准备` 一节。

您需要在 `prefect` 网络的 `4200` 端口上运行一个 Prefect 服务。

如您希望更换 Docker 网络名称或服务端口号，请同时调整 `config.toml` 中的相关配置。

启动服务：

```shell
docker compose up -d
```

## 传统部署（不推荐）

下载 Python 项目管理工具 [uv](https://github.com/astral-sh/uv)：

```shell
pip install uv
```

安装依赖库（将自动创建虚拟环境）：

```shell
uv install
```

您需要在 `8701` 端口上运行一个 Gotify 服务。

您需要在 `5173` 端口上运行一个 PostgreSQL 服务，身份验证相关信息请参考 `部署 - 数据库准备` 一节。

您需要在 `4200` 端口上运行一个 Prefect 服务。

如您希望更换服务端口号，请同时调整 `config.toml` 中的相关配置。

启动服务：

```shell
uv run main.py
```
