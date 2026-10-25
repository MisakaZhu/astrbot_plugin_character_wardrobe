# 第三方来源与许可告知

来源登记基线：`planning-2026-10-01-r1`（2026-10-01）。本随包版去除开发仓库的内部验收维护段落，第三方来源、固定提交与许可告知原文保留。

## 当前复制状态

**2026-10-01 更新：批准片段已实际复制。** `wardrobe/images.py::vision_thumb_size`（含常量 `_VISION_THUMB_SHORT_EDGE`/`_VISION_THUMB_MAX_LONG_EDGE`）复制自下列 MIT 上游，条目其余部分为本项目自编。适配差异：函数从 `ImageStore` 静态方法改为模块级 helper；调用前新增正整数与本项目上限校验（`_checked_positive_int`）；行为测试覆盖不放大、短边目标、极端长宽比与界内不变（`tests/test_images_security.py::TestVisionThumbSize`）。目标文件哈希与来源登记见开发仓库机器索引（不随本包分发）。

其余部分：AstrBot 官方源码仅作宿主 API 事实引用；AGPL 示例与无许可证候选未复制任何代码。运行依赖的实际版本、许可证和分发方式见随包 `requirements.txt`；本文件不假定尚未安装的依赖安全或已获许可。

| 原始项目 | 固定提交 | 许可 | 当前许可用途 |
| --- | --- | --- | --- |
| [Inoryu7z/astrbot_plugin_wardrobe](https://github.com/Inoryu7z/astrbot_plugin_wardrobe) | `da9e62a14b814aefdfae6a4a4417b469dd59cafd` | MIT | **已复制** `core/image_store.py::_vision_thumb_size` 纯几何逻辑至 `wardrobe/images.py`（差异与测试已登记） |
| [Soulter/helloworld](https://github.com/Soulter/helloworld) | `0c0d52b17e2feb76a7bcff887cac3d0e12eecc09` | AGPL-3.0 | 仅 API/目录惯例参考，不复制模板代码 |
| [tomjiu/astrbot_plugin_magic_wardrobe](https://github.com/tomjiu/astrbot_plugin_magic_wardrobe) | `911fdfac357ad86ac7631fe0fea5fb6e1acada11` | 未找到明确许可证 | 排除代码复用；未进行完整安全审查 |

AstrBot 官方源码仅作为宿主 API 与生命周期事实引用。运行依赖的实际版本、许可证和分发方式以随包 `requirements.txt` 为准；本文件不假定尚未安装的依赖安全或已获许可。

## 计划复用上游 MIT 原文

来源：[固定提交 LICENSE](https://github.com/Inoryu7z/astrbot_plugin_wardrobe/blob/da9e62a14b814aefdfae6a4a4417b469dd59cafd/LICENSE)。复制代码时须保留此版权及许可，不以本项目版权替换。

```text
MIT License

Copyright (c) 2026 Keiyon Inoryu7z

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
