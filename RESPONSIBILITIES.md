# 职责与依赖边界说明

本轮仅重构职责与依赖边界，不新增查询语义、不优化执行。旧解释器 `queryflow.py`
拆分为表达式核 `exprkernel.py`、关系核 `relationkernel.py` 与协调器
`coordinator.py`；`queryflow.py` 只保留导入与兼容 `run` 转发（一个函数、无类、
无解释器、无备用执行路径）。`protocol.py` 仍只放 `Fault`，不承载规则。

## 依赖方向

```
queryflow(门面) -> coordinator -> exprkernel -> protocol
                               -> relationkernel -> protocol
```

两个执行核互不导入，也不导入 coordinator、queryflow、samples；只依赖标准库
（relationkernel 用 `json` 生成分组/去重键）与 `protocol`。协调器通过模块属性
`exprkernel.evaluate` 与 `relationkernel.execute` 调用两核，可替换任一核验证
路由；协调器自身不藏解释器或回调执行器。

## 上下文（env/group/outer）归属

* 环境是从近到远的帧列表，帧是 `(alias, names, row)` 绑定列表。帧的组装与
  横向前缀（`[frame] + outer`，不含当前源自身别名）由关系核负责；名称查找
  （限定/非限定列、歧义与 UNKNOWN 判定）由表达式核独占。
* 参数 `params` 由协调器在响应 `("eval", ...)` 时交给表达式核；关系核与表达式
  核都不修改查询图、源表、参数、环境或组。
* 聚合上下文（group/aggregating）由关系核按旧代码传参：where 与 group 键为
  `group=None`，having 与普通投影带本地组；表达式核的 `("open", ...)` 不携带
  调用者的 group 或 aggregating，子查询从无组上下文重新开始，聚合上下文不跨
  查询泄漏。相关子查询经 `("open", query, outer, existence)` 沿用当前环境。

## 拉取（pull）归属

* 关系核独占表源读取（先记 `["scan", 表名]` 再查表）、横向源枚举、外关联补空、
  分组、投影顺序、去重、排序与 limit；它只求值请求、不解释表达式，where/having
  回复的布尔类型检查与过滤在关系核完成。最终行只经 `("row", row)` 交付，横向
  源的行在核内消费、绝不上交。
* 表达式核独占标量运算、短路、聚合参数求值与子查询消费规则（scalar 拉到第二
  行才报 SCALAR_MANY、in 命中即停、exists 只拉一条、空结果等旧行为）；它通过
  `("next", handle)` 逐行拉取，响应为 `("row", row)` 或 `("end",)`，失败由
  协调器 `throw(Fault)` 注入。
* 协调器独占活生成器、句柄、请求路由、异常注入与 trace 收集：响应 eval 时驱
  动表达式核到 `StopIteration.value`，响应 open/next/close 时管理关系核句柄，
  最终拼装结果或错误；除顶层 select 输出名外不解释查询键、表达式标签、操作或
  行值。limit=0 在任何读取与求值前返回；未排序达到 limit 即停止拉取。

## 关闭（close）归属

* 表达式核对每次成功 open 恰好 close 一次：正常结束、提前命中与错误路径都在
  `finally` 中发出 `("close", handle)`；关闭不新增业务 trace。
* 协调器响应 close 时关闭该关系生成器及其尚未完成的后代（按句柄父子链递归
  `gen.close()`，不推进读取）；顶层业务异常时兜底关闭全部剩余句柄。
* 关系核内部的横向子生成器由核自己在 `finally` 中关闭（级联 `close()`，清理
  路径不产生新请求）；提前结束与异常均不保留未完成后代，也不保留备用解释器。

## 实际回归结果

* `python3 checks.py` → 输出 `BUSINESS_REGRESSION_OK` 与
  `BOUNDARY_REGRESSION_OK`，退出 0（默认入口同时跑原业务回归与
  `boundary_checks.regression`）。
* `python3 boundary_checks.py` → `BOUNDARY_REGRESSION_OK`；
  `python3 boundary_checks.py business` → `ADDITIONAL_BUSINESS_OK`。
* `python3 run.py` 展示 TOTALS：`[["A",2,24],["B",0,null],["C",1,5]]`；
  `python3 checks.py demo` 输出与重构前逐字节一致。
* 差分验证：以 git 中旧解释器为基准，4000 条随机查询（含缺表、歧义、空组、
  左源、distinct/order/limit、scalar/exists/in、聚合）结果、trace 与错误完全
  一致，输入对象图不变；针对 scalar 第二行关闭、in 提前命中、exists 跳过投
  影、limit 停止拉取、深层故障级联的定向用例逐一比对一致，GC 无可观察异常。
