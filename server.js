import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import crypto from 'crypto';
import dns from 'node:dns/promises';
import { Agent, setGlobalDispatcher } from 'undici';

dotenv.config();

// 1. 网络底座配置：IPv4优先，undici 40分钟超长超时防断开
dns.setDefaultResultOrder('ipv4first');
setGlobalDispatcher(
  new Agent({
    headersTimeout: 2400000,
    bodyTimeout: 2400000,
    connectTimeout: 120000
  })
);

const app = express();
app.use(cors());
app.use(express.json({ limit: '100mb' }));

const PORT = Number(process.env.PORT || 7860);
const IS_DEBUG = (process.env.DEBUG || 'false').toLowerCase() === 'true';

// ==========================================
// 1. 结构化增量日志
// ==========================================
function getTimestamp() {
  return new Date().toISOString().replace('T', ' ').substring(0, 19);
}

const logger = {
  debug: (stage, details = {}) => {
    if (!IS_DEBUG) return;
    console.log(`\n\x1b[36m[${getTimestamp()}]\x1b[0m \x1b[32m【流程: ${stage}】\x1b[0m`);
    for (const [k, v] of Object.entries(details)) {
      if (v !== undefined && v !== null) {
        const valStr = typeof v === 'object' ? JSON.stringify(v, null, 2) : String(v);
        console.log(`  \x1b[33m▶ ${k}:\x1b[0m ${valStr}`);
      }
    }
  },
  error: (stage, err) => {
    console.error(`\n\x1b[31m[${getTimestamp()}] 【错误: ${stage}】\x1b[0m`, err);
  }
};

// ==========================================
// 2. 动态 URL 穿透解析
// ==========================================
function parseTargetUrl(req) {
  let raw = req.originalUrl.startsWith('/') ? req.originalUrl.slice(1) : req.originalUrl;
  if (!/^https?:\/\//i.test(raw)) {
    try { raw = decodeURIComponent(raw); } catch {}
  }

  const v1Match = raw.match(/^(https?:\/\/[^\/]+(?:\/[^\/]+)*?)\/(v1\/(?:messages|chat\/completions|models|messages\/count_tokens))(?:\?(.*))?$/i);
  if (v1Match) {
    return {
      upstreamBase: v1Match[1],
      endpoint: '/' + v1Match[2],
      fullTarget: v1Match[1] + '/' + v1Match[2] + (v1Match[3] ? '?' + v1Match[3] : '')
    };
  }

  return {
    upstreamBase: (process.env.UPSTREAM_BASE_URL || '').replace(/\/$/, ''),
    endpoint: req.path,
    fullTarget: (process.env.UPSTREAM_BASE_URL || '').replace(/\/$/, '') + req.path
  };
}

// ==========================================
// 3. Action 与 Claude Code 原生工具适配映射器
// ==========================================
function mapActionToClaudeCodeTool(actionName, rawParams) {
  const normAction = String(actionName || '').trim().toLowerCase();
  const params = rawParams || {};

  if (normAction === 'fs_write' || normAction === 'write') {
    return {
      name: 'Write',
      arguments: {
        file_path: params.file_path || params.path || 'temp.txt',
        content: params.content !== undefined ? params.content : ''
      }
    };
  }

  if (normAction === 'fs_read' || normAction === 'read') {
    return {
      name: 'Read',
      arguments: {
        file_path: params.file_path || params.path || '',
        ...(params.limit ? { limit: Number(params.limit) } : {}),
        ...(params.offset ? { offset: Number(params.offset) } : {})
      }
    };
  }

  if (normAction === 'fs_replace' || normAction === 'edit') {
    return {
      name: 'Edit',
      arguments: {
        file_path: params.file_path || params.path || '',
        old_string: params.old_string !== undefined ? params.old_string : '',
        new_string: params.new_string !== undefined ? params.new_string : '',
        replace_all: Boolean(params.replace_all)
      }
    };
  }

  if (normAction === 'shell_exec' || normAction === 'bash') {
    return {
      name: 'Bash',
      arguments: {
        command: params.command || params.cmd || '',
        ...(params.description ? { description: params.description } : {})
      }
    };
  }

  if (normAction === 'user_prompt' || normAction === 'askuserquestion') {
    let questions = [];
    if (Array.isArray(params.questions)) {
      questions = params.questions;
    } else {
      const qText = params.question || params.prompt || '请确认下一步操作：';
      const rawOptions = Array.isArray(params.options) ? params.options : ['确认', '取消'];
      const formattedOptions = rawOptions.map(opt => {
        if (typeof opt === 'string') return { label: opt, description: opt };
        return { label: opt.label || '选项', description: opt.description || opt.label || '' };
      });

      questions = [{
        question: qText,
        header: params.header || '中介决策确认',
        multiSelect: Boolean(params.multiSelect),
        options: formattedOptions
      }];
    }
    return { name: 'AskUserQuestion', arguments: { questions } };
  }

  if (normAction === 'net_search' || normAction === 'websearch') {
    return { name: 'WebSearch', arguments: { query: params.query || '' } };
  }

  if (normAction === 'net_fetch' || normAction === 'webfetch') {
    return { name: 'WebFetch', arguments: { url: params.url || '', prompt: params.prompt || '提取关键内容' } };
  }

  if (normAction === 'subflow_spawn' || normAction === 'agent') {
    return {
      name: 'Agent',
      arguments: {
        description: params.title || params.description || 'Sub-agent task',
        prompt: params.instructions || params.prompt || ''
      }
    };
  }

  if (normAction === 'task_entry' || normAction === 'taskcreate' || normAction === 'taskupdate') {
    if (params.action === 'update' || params.taskId) {
      return {
        name: 'TaskUpdate',
        arguments: {
          taskId: params.taskId || params.task_id,
          status: params.status || 'completed'
        }
      };
    }
    return {
      name: 'TaskCreate',
      arguments: {
        subject: params.title || params.subject || '任务',
        description: params.description || ''
      }
    };
  }

  if (normAction === 'notebook_patch' || normAction === 'notebookedit') {
    return {
      name: 'NotebookEdit',
      arguments: {
        notebook_path: params.notebook_path || '',
        cell_id: params.cell_id || '',
        edit_mode: params.edit_mode || 'replace',
        new_source: params.new_source || ''
      }
    };
  }

  if (normAction === 'git_worktree' || normAction === 'enterworktree') {
    return {
      name: 'EnterWorktree',
      arguments: {
        name: params.name || 'worktree',
        path: params.path || ''
      }
    };
  }

  if (normAction === 'code_audit' || normAction === 'reportfindings') {
    return {
      name: 'ReportFindings',
      arguments: {
        findings: params.findings || [],
        level: params.level || 'medium'
      }
    };
  }

  return { name: 'Bash', arguments: params };
}

// ==========================================
// 4. 对话历史解析与智能压缩引擎
// ==========================================

const CORE_DOCS_REGEX = /(?:^|[/\s"'\`\\])(?:todo|readme)\.(?:md|markdown|txt)(?:[/\s"'\`\\]|$)/i;

function sanitizeWhitespace(text) {
  if (!text || typeof text !== 'string') return '';
  return text
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .replace(/[ \t]+$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function cleanNoise(text) {
  if (!text || typeof text !== 'string') return '';

  text = cleanClaudeCodeCompactionText(text);
  
  const cleaned = text
    .replace(/REMINDER:\s*You MUST include the sources[\s\S]*?hyperlinks\./gi, '')
    .replace(/Wasted call\s*—\s*file unchanged[\s\S]*?instead\./gi, '[SUCCESS] 文件未修改，状态已是最新。')
    .replace(/<system-reminder>[\s\S]*?<\/system-reminder>/gi, '')
    .replace(/<total_tokens>[\s\S]*?<\/total_tokens>/gi, '')
    .replace(/<task-notification>[\s\S]*?<\/task-notification>/gi, '')
    .replace(/<context>[\s\S]*?<\/context>/gi, '');
  return sanitizeWhitespace(cleaned);
}

function cleanClaudeCodeCompactionText(text) {
  if (!text || typeof text !== 'string') return '';

  let s = String(text);

  const isCCCompaction =
    /This session is being continued from a previous conversation/i.test(s) ||
    /ran out of context/i.test(s) ||
    /read the full transcript at:/i.test(s) ||
    /\.claude\/projects\/.*\.jsonl/i.test(s);

  if (!isCCCompaction) return text;

  // 删除 transcript 路径和平台痕迹
  s = s
    .replace(/This session is being continued from a previous conversation that ran out of context\.[\s\S]*?summary below covers the earlier portion of the conversation\./gi, '')
    .replace(/If you need specific details[\s\S]*?\.jsonl/gi, '')
    .replace(/Continue the conversation[\s\S]*?(similar\.|break never happened\.)/gi, '')
    .replace(/\/[^\s]*\.claude\/projects\/[^\s]+\.jsonl/gi, '')
    .replace(/Claude Code/gi, '本地自动化工具')
    .trim();

  if (!s) {
    return '这是一次上下文压缩后的续接任务。请根据已有任务摘要和历史执行记录继续推进，不要询问用户，不要重复初始化。';
  }

  return `这是一次上下文压缩后的续接任务。请根据以下摘要继续推进，不要询问用户，不要重复初始化。\n\n${s}`;
}

function stringifyUserContent(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.map(c => {
      if (!c) return '';
      if (typeof c === 'string') return c;
      if (c.type === 'text') return c.text || '';
      if (c.text) return c.text;
      if (c.content && typeof c.content === 'string') return c.content;
      return '';
    }).filter(Boolean).join('\n');
  }
  if (content && typeof content === 'object') {
    try { return JSON.stringify(content); } catch { return String(content); }
  }
  return '';
}

function smartTruncateLog(text, limit, label = '终端日志') {
  if (!text || text.length <= limit) return text || '';

  const headSize = Math.max(500, Math.floor(limit * 0.35));
  const tailSize = Math.max(600, Math.floor(limit * 0.45));

  const headPart = text.slice(0, headSize);
  const tailPart = text.slice(-tailSize);
  const middleContent = text.slice(headSize, -tailSize);

  const lines = middleContent.split('\n');
  const errorIndicators = [
    /error/i,
    /exception/i,
    /fail/i,
    /failed/i,
    /traceback/i,
    /exit code\s*[1-9]/i,
    /cannot access/i,
    /no such file/i,
    /permission denied/i,
    /syntaxerror/i,
    /typeerror/i,
    /referenceerror/i,
    /warning/i,
    /deprecated/i
  ];
  const capturedLines = [];

  for (let i = 0; i < lines.length && capturedLines.length < 25; i++) {
    if (errorIndicators.some(reg => reg.test(lines[i]))) {
      capturedLines.push(lines[i].trim());
    }
  }

  const removed = text.length - headSize - tailSize;
  let summary = `\n...[${label}中间输出已折叠 ${removed} 字符`;
  if (capturedLines.length > 0) {
    summary += `，提取关键异常信号：\n${capturedLines.slice(0, 12).join('\n')}\n...折叠结束]...\n`;
  } else {
    summary += `]...\n`;
  }

  return `${headPart}${summary}${tailPart}`;
}

function extractImportantLines(text, maxLines = 40) {
  if (!text) return '';
  const lines = String(text).split('\n').map(x => x.trim()).filter(Boolean);
  const regs = [
    /error/i,
    /exception/i,
    /fail/i,
    /failed/i,
    /traceback/i,
    /exit code\s*[1-9]/i,
    /no such file/i,
    /permission denied/i,
    /success/i,
    /created/i,
    /updated/i,
    /modified/i,
    /deleted/i,
    /passed/i,
    /test/i,
    /build/i,
    /lint/i,
    /warning/i,
    /todo/i,
    /done/i,
    /completed/i
  ];
  const picked = lines.filter(line => regs.some(r => r.test(line)));
  return picked.slice(0, maxLines).join('\n');
}

function summarizeParamsByAction(actionName, params = {}) {
  const action = String(actionName || '').toLowerCase();
  const p = { ...params };

  if (action === 'fs_write') {
    const filePath = p.file_path || p.path || 'file';
    const content = String(p.content || '');
    const isTodo = /(?:^|[/\\])todo\.(?:md|markdown|txt)$/i.test(filePath);
    const isReadme = /(?:^|[/\\])readme\.(?:md|markdown|txt)$/i.test(filePath);

    if (isTodo) {
      return { file_path: filePath, content };
    }

    if (isReadme) {
      return {
        file_path: filePath,
        content: content.length <= 6000 ? content : `[readme.md 已写入，共 ${content.length} 字符。摘要保留：]\n${smartTruncateLog(content, 6000, 'readme文档')}`
      };
    }

    return {
      file_path: filePath,
      content_summary: `[已写入文件，共 ${content.length} 字符]`,
      content_preview: content.length <= 1200 ? content : `${content.slice(0, 700)}\n...[源码中段省略]...\n${content.slice(-400)}`
    };
  }

  if (action === 'fs_replace') {
    const filePath = p.file_path || p.path || 'file';
    const oldStr = String(p.old_string || '');
    const newStr = String(p.new_string || '');
    const isCore = /(?:^|[/\\])(?:todo|readme)\.(?:md|markdown|txt)$/i.test(filePath);

    if (isCore) {
      return {
        ...p,
        old_string: oldStr.length <= 3000 ? oldStr : smartTruncateLog(oldStr, 3000, '核心文档旧内容'),
        new_string: newStr.length <= 6000 ? newStr : smartTruncateLog(newStr, 6000, '核心文档新内容')
      };
    }

    return {
      file_path: filePath,
      old_string: oldStr.length <= 500 ? oldStr : `${oldStr.slice(0, 240)}...[略]...${oldStr.slice(-160)}`,
      new_string: newStr.length <= 800 ? newStr : `${newStr.slice(0, 360)}...[略]...${newStr.slice(-240)}`,
      replace_all: Boolean(p.replace_all)
    };
  }

  if (action === 'fs_read') {
    return {
      file_path: p.file_path || p.path || '',
      ...(p.limit ? { limit: p.limit } : {}),
      ...(p.offset ? { offset: p.offset } : {})
    };
  }

  if (action === 'shell_exec') {
    return {
      command: p.command || p.cmd || '',
      ...(p.description ? { description: p.description } : {})
    };
  }

  if (action === 'user_prompt') {
    return p;
  }

  if (action === 'net_search') {
    return { query: p.query || '' };
  }

  if (action === 'net_fetch') {
    return { url: p.url || '', prompt: p.prompt || '' };
  }

  if (action === 'subflow_spawn') {
    return {
      title: p.title || p.description || '',
      instructions: p.instructions || p.prompt || ''
    };
  }

  if (action === 'task_entry') {
    return p;
  }

  if (action === 'notebook_patch') {
    return {
      notebook_path: p.notebook_path || '',
      cell_id: p.cell_id || '',
      edit_mode: p.edit_mode || 'replace',
      new_source: String(p.new_source || '').length <= 1500
        ? String(p.new_source || '')
        : `${String(p.new_source || '').slice(0, 900)}\n...[notebook源码省略]...\n${String(p.new_source || '').slice(-400)}`
    };
  }

  return p;
}

function classifyStepRetention(actionName, params = {}, feedback = '') {
  const action = String(actionName || '').toLowerCase();
  const fp = String(params.file_path || params.path || '');
  const cmd = String(params.command || params.cmd || '');
  const text = String(feedback || '');

  const isTodo = /(?:^|[/\\])todo\.(?:md|markdown|txt)$/i.test(fp);
  const isReadme = /(?:^|[/\\])readme\.(?:md|markdown|txt)$/i.test(fp);
  const isCoreDoc = isTodo || isReadme || CORE_DOCS_REGEX.test(fp) || CORE_DOCS_REGEX.test(cmd);
  const hasChecklist = /- \[[ xX]\]/m.test(text);
  const hasError = /error|exception|failed|fail|traceback|exit code\s*[1-9]|permission denied|no such file/i.test(text);
  const hasSuccessSignal = /success|created|updated|modified|written|completed|done|passed/i.test(text);

  if (action === 'user_prompt') {
    return {
      tier: 'must_keep_full',
      reason: '用户决策问答必须完整保留，避免后续推进丢失用户选择。'
    };
  }

  if (isCoreDoc || hasChecklist) {
    return {
      tier: 'core_context',
      reason: 'todo/readme/清单类核心项目状态必须高保真保留。'
    };
  }

  if (action === 'fs_read') {
    return {
      tier: isCoreDoc ? 'core_context' : 'contextual',
      reason: isCoreDoc ? '核心文档读取。' : '普通文件读取保留关键片段，避免重复上下文过大。'
    };
  }

  if (action === 'fs_write' || action === 'fs_replace' || action === 'notebook_patch') {
    return {
      tier: 'project_progress',
      reason: '文件变更会影响项目状态，保留路径、变更摘要与关键反馈。'
    };
  }

  if (action === 'shell_exec') {
    if (hasError) {
      return {
        tier: 'diagnostic',
        reason: '命令失败/异常对下一步修复非常关键，保留错误上下文。'
      };
    }
    if (/npm|pnpm|yarn|bun|pytest|test|lint|build|tsc|eslint|vitest|jest|mvn|gradle|go test|cargo test|python|node/i.test(cmd)) {
      return {
        tier: 'validation',
        reason: '构建/测试/检查结果用于判断是否继续推进。'
      };
    }
    if (/ls|dir|find|grep|rg|cat|tree|pwd|git status|git diff/i.test(cmd)) {
      return {
        tier: 'inspection',
        reason: '环境探查命令保留结构与关键结果即可。'
      };
    }
    return {
      tier: 'light',
      reason: hasSuccessSignal ? '普通成功命令只需保留摘要。' : '普通命令保留关键输出。'
    };
  }

  if (action === 'net_search' || action === 'net_fetch') {
    return {
      tier: 'knowledge',
      reason: '网络检索内容可能指导实现，保留结论、链接与关键事实。'
    };
  }

  if (action === 'subflow_spawn') {
    return {
      tier: 'project_progress',
      reason: '子任务结果可能包含实现建议与验收结论。'
    };
  }

  if (action === 'task_entry') {
    return {
      tier: 'project_progress',
      reason: '任务创建/更新属于项目推进状态。'
    };
  }

  if (action === 'code_audit') {
    return {
      tier: 'diagnostic',
      reason: '代码审计结果对修复和验收关键。'
    };
  }

  return {
    tier: 'light',
    reason: '未知或低价值工具，保留摘要和异常信号。'
  };
}

function formatLocalFeedback(str, actionName, stepParams = {}, isLatestStep = false, stepAge = 0) {
  if (!str) return '[SUCCESS] 操作已执行完成';

  let text = sanitizeWhitespace(String(str));
  const retention = classifyStepRetention(actionName, stepParams, text);
  const action = String(actionName || '').toLowerCase();

  if (/successfully|created|updated|done|completed|written|modified/i.test(text) && !text.startsWith('[')) {
    text = `[SUCCESS] ${text}`;
  }

  if (retention.tier === 'must_keep_full') {
    return text.length <= 30000 ? text : smartTruncateLog(text, 30000, '用户问答结果');
  }

  if (retention.tier === 'core_context') {
    return text.length <= 22000 ? text : smartTruncateLog(text, 22000, '核心任务/设计文档');
  }

  if (retention.tier === 'diagnostic') {
    return text.length <= 18000 ? text : smartTruncateLog(text, 18000, '异常诊断日志');
  }

  if (retention.tier === 'validation') {
    const budget = isLatestStep ? 14000 : stepAge <= 2 ? 8000 : 5000;
    return text.length <= budget ? text : smartTruncateLog(text, budget, '构建/测试/校验输出');
  }

  if (retention.tier === 'knowledge') {
    const budget = isLatestStep ? 12000 : stepAge <= 2 ? 7000 : 4500;
    return text.length <= budget ? text : smartTruncateLog(text, budget, '网络知识结果');
  }

  if (retention.tier === 'project_progress') {
    const budget = isLatestStep ? 12000 : stepAge <= 2 ? 7000 : 4000;
    return text.length <= budget ? text : smartTruncateLog(text, budget, '项目变更反馈');
  }

  if (retention.tier === 'inspection') {
    const important = extractImportantLines(text, 30);
    const budget = isLatestStep ? 8000 : stepAge <= 2 ? 4000 : 2200;
    if (text.length <= budget) return text;
    if (important) {
      return `${smartTruncateLog(text, budget, '环境探查输出')}\n\n【提取的关键行】:\n${important}`;
    }
    return smartTruncateLog(text, budget, '环境探查输出');
  }

  const budget = isLatestStep ? 6000 : stepAge <= 2 ? 3000 : 1600;
  if (action === 'fs_write') {
    return text.length <= 1800 ? text : smartTruncateLog(text, 1800, '写入反馈');
  }
  return text.length <= budget ? text : smartTruncateLog(text, budget, '低价值工具反馈');
}

function normalizeUserAnswerText(text) {
  const cleaned = cleanNoise(text);
  if (!cleaned) return '';
  if (cleaned.startsWith("Today's date is")) return '';
  if (/^\s*\{[\s\S]*\}\s*$/.test(cleaned) && cleaned.length > 5000) {
    return smartTruncateLog(cleaned, 8000, '用户结构化回答');
  }
  return cleaned;
}

function compressHistorySteps(rawSteps) {
  const validSteps = (rawSteps || []).filter(s => s.action && s.action !== 'text_response');

  if (validSteps.length === 0) {
    return '（当前为初始化阶段，尚无历史记录）';
  }

  const latestOriginalIdx = validSteps.length - 1;
  const mustKeepIndexes = new Set();

  // 1. 最后一步无条件保留
  mustKeepIndexes.add(latestOriginalIdx);

  // 2. 用户问答、核心上下文必须保留
  validSteps.forEach((s, idx) => {
    const retention = classifyStepRetention(s.action, s.params || {}, s.feedback || '');
    if (
      retention.tier === 'must_keep_full' ||
      retention.tier === 'core_context' ||
      s.action === 'user_prompt'
    ) {
      mustKeepIndexes.add(idx);
    }
  });

  // 3. 最近 6 轮保留
  const recentStart = Math.max(0, validSteps.length - 6);
  for (let i = recentStart; i < validSteps.length; i++) {
    mustKeepIndexes.add(i);
  }

  const importantActions = new Set([
    'fs_write',
    'fs_replace',
    'notebook_patch',
    'shell_exec',
    'net_search',
    'net_fetch',
    'subflow_spawn',
    'task_entry',
    'code_audit'
  ]);

  // 4. 错误步骤强制保留
  validSteps.forEach((s, idx) => {
    if (!importantActions.has(s.action)) return;
    const feedback = String(s.feedback || '');
    if (/error|exception|failed|fail|traceback|exit code\s*[1-9]|permission denied|no such file/i.test(feedback)) {
      mustKeepIndexes.add(idx);
    }
  });

  // 5. 统计每个文件最后一次读取，基于完整 validSteps，不基于渲染列表
  const lastReadOriginalMap = new Map();

  validSteps.forEach((s, originalIdx) => {
    if (s.action === 'fs_read') {
      const fp = String(s.params?.file_path || s.params?.path || '').toLowerCase();
      if (fp) {
        lastReadOriginalMap.set(fp, originalIdx);
      }
    }
  });

  const selectedIndexes = [...mustKeepIndexes].sort((a, b) => a - b);

  // 最大上下文保留轮数
  const maxHistorical = 10;
  let indexesToRender = selectedIndexes;

  if (selectedIndexes.length > maxHistorical) {
    const must = selectedIndexes.filter(i => {
      const s = validSteps[i];
      const retention = classifyStepRetention(s.action, s.params || {}, s.feedback || '');
      return (
        retention.tier === 'must_keep_full' ||
        retention.tier === 'core_context' ||
        i >= recentStart ||
        i === latestOriginalIdx
      );
    });

    const rest = selectedIndexes.filter(i => !must.includes(i));

    const restRoom = Math.max(0, maxHistorical - must.length);

    indexesToRender = [
      ...rest.slice(-restRoom),
      ...must
    ].sort((a, b) => a - b);
  }

  const omittedCount = validSteps.length - indexesToRender.length;
  const total = indexesToRender.length;

  const rendered = indexesToRender.map((originalIdx, renderIdx) => {
    const step = validSteps[originalIdx];
    let feedback = step.feedback || '[SUCCESS] 执行完成';
    let params = summarizeParamsByAction(step.action, step.params || {});
    const filePathStr = String(params.file_path || params.path || '');

    const isTodoFile = /(?:^|[/\\])todo\.(?:md|markdown|txt)$/i.test(filePathStr);
    const isReadmeFile = /(?:^|[/\\])readme\.(?:md|markdown|txt)$/i.test(filePathStr);

    // 注意：这里用原始索引算 age，不用 renderIdx
    const stepAge = latestOriginalIdx - originalIdx;
    const isLatestStep = originalIdx === latestOriginalIdx;

    const retention = classifyStepRetention(step.action, params, feedback);

    if (step.action === 'fs_read') {
      const lowerPath = filePathStr.toLowerCase();
      const latestReadOriginalIdx = lastReadOriginalMap.get(lowerPath);

      // 最后一步：不管是什么，绝不压缩
      if (isLatestStep) {
        feedback = sanitizeWhitespace(String(feedback));
      }
      // 普通文件读取：后面有同文件更新读取，则旧的折叠
      else if (
        latestReadOriginalIdx !== undefined &&
        latestReadOriginalIdx > originalIdx &&
        !isTodoFile &&
        !isReadmeFile
      ) {
        feedback = `[早期版本已读取，后续有同文件最新读取结果，此处折叠。文件：${filePathStr}]`;
      }
      // 其他读取正常压缩
      else {
        feedback = formatLocalFeedback(feedback, 'fs_read', params, isLatestStep, stepAge);
      }
    } else {
      // 最后一步：不管是什么，绝不压缩
      if (isLatestStep) {
        feedback = sanitizeWhitespace(String(feedback));
      } else {
        feedback = formatLocalFeedback(feedback, step.action, params, isLatestStep, stepAge);
      }
    }

    return `--- Step ${renderIdx + 1} / 原始第 ${originalIdx + 1} 步 ---
【保留级别】：${retention.tier}
【保留原因】：${retention.reason}
【执行配置】：
${JSON.stringify({
  action: step.action,
  params
}, null, 2)}
【本地执行反馈 / 用户回答】：
${feedback}`;
  }).join('\n\n');

  const finalText = omittedCount > 0
    ? `【历史压缩说明】：原始共有 ${validSteps.length} 个工具/问答步骤，已智能保留 ${indexesToRender.length} 个关键步骤，折叠 ${omittedCount} 个低价值或过旧步骤；用户问答、todo/readme、错误、测试、文件变更均优先保留，最后一步永不压缩。\n\n${rendered}`
    : rendered;

  // 关键：不要用 hardLimitText 的 head/tail，否则可能破坏最新步骤。
  // 这里强制保留尾部，也就是最新历史。
  if (finalText.length <= MAX_HISTORY_CHARS) {
    return finalText;
  }

  return `【历史过长，已保留最新部分，避免截断最后一步】\n\n${finalText.slice(-MAX_HISTORY_CHARS)}`;
}

function parseConversation(messages = []) {
  let globalTask = '';
  const rawSteps = [];

  const CC_TO_ACTION_MAP = {
    Write: 'fs_write',
    Read: 'fs_read',
    Edit: 'fs_replace',
    Bash: 'shell_exec',
    WebSearch: 'net_search',
    WebFetch: 'net_fetch',
    AskUserQuestion: 'user_prompt',
    Agent: 'subflow_spawn',
    Workflow: 'subflow_spawn',
    TaskCreate: 'task_entry',
    TaskUpdate: 'task_entry',
    NotebookEdit: 'notebook_patch',
    EnterWorktree: 'git_worktree',
    ReportFindings: 'code_audit'
  };

  for (const msg of messages) {
    if (msg.role === 'user') {
      const rawText = stringifyUserContent(msg.content);
      const clean = cleanNoise(rawText);
      if (
        clean &&
        !clean.startsWith('<tool_result') &&
        !clean.includes("Today's date is") &&
        !clean.startsWith('{') &&
        !/^\s*(selected|answer|choice|option|确认|取消|yes|no)\s*[:：]/i.test(clean)
      ) {
        globalTask = clean;
        break;
      }
    }
  }
  if (!globalTask) globalTask = '推进当前工作目录下的任务推进。';

  // 最新用户中途消息：解决“用户想先沟通，但系统继续死板推进”的问题
  // 只取第 2 条之后的普通 user 文本，避免把初始任务当成打断消息
  let latestUserMessage = '';

  for (let i = messages.length - 1; i >= 1; i--) {
    const msg = messages[i];
    if (msg.role !== 'user') continue;

    const rawText = stringifyUserContent(msg.content);
    const clean = cleanNoise(rawText);

    if (
      clean &&
      !clean.startsWith('<tool_result') &&
      !clean.includes("Today's date is") &&
      !/^\s*\{[\s\S]*\}\s*$/.test(clean) &&
      !/^\s*(selected|answer|choice|option|确认|取消|yes|no)\s*[:：]/i.test(clean)
    ) {
      latestUserMessage = hardLimitText(clean, 6000, '最新用户中途消息');
      break;
    }
  }

  if (latestUserMessage) {
    globalTask = `${globalTask}

【最新用户中途消息，最高优先级】：
${latestUserMessage}

【中途消息处理要求】：
如果这条消息是在沟通方案、补充需求、修改方向、暂停执行、提问、纠正、吐槽、要求确认，必须先响应用户，不得继续推进旧计划。
如果这条消息明确表示“继续、按方案执行、确认、可以、开始”，才允许继续执行下一步。`;
  }

  const pendingSteps = new Map();
  const sequentialQueue = [];
  let awaitingUserAnswerStep = null;

  const isLikelyUserAnswerToQuestion = (text) => {
    const clean = normalizeUserAnswerText(text);
    if (!clean) return false;
    if (!awaitingUserAnswerStep) return false;
    if (clean.startsWith('<tool_result')) return false;
    if (clean.includes("Today's date is")) return false;
    return true;
  };

  const matchAndPopStep = (toolCallId) => {
    if (toolCallId && pendingSteps.has(toolCallId)) {
      const step = pendingSteps.get(toolCallId);
      pendingSteps.delete(toolCallId);
      const qIdx = sequentialQueue.findIndex(s => s.id === toolCallId);
      if (qIdx !== -1) sequentialQueue.splice(qIdx, 1);
      return step;
    }
    return sequentialQueue.shift() || null;
  };

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];

    if (msg.role === 'assistant') {
      if (Array.isArray(msg.content)) {
        for (const p of msg.content) {
          if (p.type === 'tool_use') {
            const mappedAction = CC_TO_ACTION_MAP[p.name] || 'shell_exec';
            const stepObj = {
              id: p.id || '',
              action: mappedAction,
              params: p.input || {}
            };
            if (p.id) pendingSteps.set(p.id, stepObj);
            sequentialQueue.push(stepObj);
            if (mappedAction === 'user_prompt') awaitingUserAnswerStep = stepObj;
          }
        }
      }

      if (msg.tool_calls && Array.isArray(msg.tool_calls)) {
        for (const tc of msg.tool_calls) {
          const fnName = tc.function?.name || '';
          const mappedAction = CC_TO_ACTION_MAP[fnName] || 'shell_exec';
          let params = {};
          try {
            params = typeof tc.function?.arguments === 'string'
              ? JSON.parse(tc.function.arguments)
              : (tc.function?.arguments || {});
          } catch {
            params = {};
          }
          const stepObj = {
            id: tc.id || '',
            action: mappedAction,
            params
          };
          if (tc.id) pendingSteps.set(tc.id, stepObj);
          sequentialQueue.push(stepObj);
          if (mappedAction === 'user_prompt') awaitingUserAnswerStep = stepObj;
        }
      }

      if (typeof msg.content === 'string' && msg.content.includes('<tool_call>')) {
        const tcMatch = msg.content.match(/<tool_call>\s*([\s\S]*?)\s*<\/tool_call>/i);
        if (tcMatch) {
          try {
            const parsed = JSON.parse(cleanLooseJson(tcMatch[1]));
            if (parsed.name) {
              const mappedAction = CC_TO_ACTION_MAP[parsed.name] || 'shell_exec';
              const stepObj = {
                id: '',
                action: mappedAction,
                params: parsed.arguments || {}
              };
              sequentialQueue.push(stepObj);
              if (mappedAction === 'user_prompt') awaitingUserAnswerStep = stepObj;
            }
          } catch {}
        }
      }
    } else if (msg.role === 'user' || msg.role === 'tool') {
      if (Array.isArray(msg.content)) {
        for (const p of msg.content) {
          if (p.type === 'tool_result') {
            const outText = typeof p.content === 'string'
              ? p.content
              : (Array.isArray(p.content) ? p.content.map(c => c.text || '').join('\n') : stringifyUserContent(p.content));
            const matchedStep = matchAndPopStep(p.tool_use_id);
            if (matchedStep) {
              const cleanFeedback = cleanNoise(outText);
              rawSteps.push({ ...matchedStep, feedback: cleanFeedback });
              if (matchedStep.action === 'user_prompt') {
                awaitingUserAnswerStep = null;
              }
            }
          } else if (p.type === 'text') {
            const answerText = normalizeUserAnswerText(p.text || '');
            if (isLikelyUserAnswerToQuestion(answerText)) {
              const idx = sequentialQueue.indexOf(awaitingUserAnswerStep);
              if (idx !== -1) sequentialQueue.splice(idx, 1);
              if (awaitingUserAnswerStep.id) pendingSteps.delete(awaitingUserAnswerStep.id);
              rawSteps.push({
                ...awaitingUserAnswerStep,
                feedback: answerText,
                user_answer: answerText
              });
              awaitingUserAnswerStep = null;
            }
          }
        }
      } else if (msg.role === 'tool' && msg.tool_call_id) {
        const outText = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content);
        const matchedStep = matchAndPopStep(msg.tool_call_id);
        if (matchedStep) {
          rawSteps.push({ ...matchedStep, feedback: cleanNoise(outText) });
          if (matchedStep.action === 'user_prompt') {
            awaitingUserAnswerStep = null;
          }
        }
      } else if (typeof msg.content === 'string') {
        const text = cleanNoise(msg.content);

        if (isLikelyUserAnswerToQuestion(text)) {
          const answerText = normalizeUserAnswerText(text);
          const idx = sequentialQueue.indexOf(awaitingUserAnswerStep);
          if (idx !== -1) sequentialQueue.splice(idx, 1);
          if (awaitingUserAnswerStep.id) pendingSteps.delete(awaitingUserAnswerStep.id);
          rawSteps.push({
            ...awaitingUserAnswerStep,
            feedback: answerText,
            user_answer: answerText
          });
          awaitingUserAnswerStep = null;
        } else if (text && !text.startsWith("Today's date is") && sequentialQueue.length > 0) {
          const matchedStep = matchAndPopStep(null);
          if (matchedStep) {
            rawSteps.push({ ...matchedStep, feedback: text });
            if (matchedStep.action === 'user_prompt') {
              awaitingUserAnswerStep = null;
            }
          }
        }
      }
    }
  }

  globalTask = hardLimitText(globalTask, MAX_GLOBAL_TASK_CHARS, '全局目标任务');

  const historyLogsText = compressHistorySteps(rawSteps);
  const latestTurnInput = rawSteps.length > 0
    ? hardLimitText(rawSteps[rawSteps.length - 1].feedback, 8000, '最新执行反馈')
    : '（初始启动任务）';

  return { globalTask, historyLogsText, latestTurnInput };
}

// ==========================================
// 5. 强化版抗风控 Prompt
// ==========================================
function buildPrompt(globalTask, historyLogsText) {
  return `我们正在维护一个本地自动化工作流引擎。该引擎按流水线（Pipeline）逐步执行任务。每个步骤由你根据历史日志输出一个独立的任务指令块，由外部执行脚本读取并在本地操作系统中执行，执行完毕后会将终端标准输出或用户回答作为日志反馈给你。

【流水线可用指令库（Action Library）】：
1. 文件与代码管理：
   - fs_write: {"file_path": "路径", "content": "完整文本"}
   - fs_read: {"file_path": "路径"}
   - fs_replace: {"file_path": "路径", "old_string": "待换原文本", "new_string": "新文本"}
   - notebook_patch: {"notebook_path": "路径", "cell_id": "单元格ID", "edit_mode": "replace|insert|delete", "new_source": "代码"}
2. 系统与环境交互：
   - shell_exec: {"command": "终端Shell命令"}
   - user_prompt: {"question": "需用户决策的问题", "options": ["选项1", "选项2"]}
   - git_worktree: {"action": "enter|exit", "path": "隔离工作区路径"}
3. 网络与知识检索：
   - net_search: {"query": "搜索词"}
   - net_fetch: {"url": "网址", "prompt": "提取目标"}
4. 任务编排与治理：
   - task_entry: {"action": "create|update", "title": "任务名", "status": "pending|completed"}
   - subflow_spawn: {"title": "子任务名", "instructions": "分派执行说明"}
   - code_audit: {"findings": [{"file": "文件", "summary": "问题描述", "verdict": "CONFIRMED"}]}
5. 流程终结：
   - finish: {"summary": "全部流水线验收完成后的总结报告"}

【流水线设计约束】：
1. 拆解规范：当工作流初次启动（无历史记录）时，先检查本地目录是否有 todo.md 和 readme.md 文件：
- 若都有，检查相关内容是否与任务一致，一致则继续推进todo.md，不一致就算没有；
- 只要有任何一个没有，第一个步骤必须对任务进行极细致的拆解（具体到单文件、单页面或单步骤），输出一个 action 为 "fs_write" 的配置，将任务项全为 [ ] 的 todo.md 写入本地，并将具体情况规划方案等写入本地 readme.md （ readme.md 要让完全不了解项目的看了都能明白）。
2. 单步原则：每个回复只能输出当前唯一步骤的配置，不可合并多个步骤。
3. 用户问答原则：历史记录里的 user_prompt 反馈代表用户真实选择/回答，必须严格继承，不得重复询问已回答的问题，除非答案无法执行。
4. 终止条件：当且仅当所有待办项均已完成验收时，输出 action 为 "finish" 的收尾配置。
5. 格式严律：【思考】与【调度动作】必须严格按照模板给出，json 代码块中必须为合法 JSON（字符串内部换行必须转义为 \\n，不要打回车换行）。

【强制返回格式模板示例】:
【思考】: 说明当前步骤的意图与判断分析...
【调度动作】: fs_write
\`\`\`json
{
  "file_path": "todo.md",
  "content": "# 任务清单\\n- [ ] 步骤一\\n- [ ] 步骤二"
}
\`\`\`

=======================================================
【全局目标任务】：
${globalTask}
=======================================================
【历史执行记录】：
${historyLogsText}
=======================================================
【当前调度决策】：
请综合【全局目标任务】与【历史执行记录】，评估当前阶段并输出下一步操作：

【最高优先级规则】：
- 如果【全局目标任务】里包含“最新用户中途消息”，必须先判断它是不是用户在沟通、补充、纠正、暂停、提问、要求确认或表达不满。
- 若是，则不得继续执行 fs_read/fs_write/fs_replace/shell_exec 等本地推进动作。
- 此时必须输出 user_prompt，把你的理解、方案或需要确认的问题写进 question，等待用户确认。
- 只有用户明确表示“继续、确认、可以、开始、按方案执行”时，才允许继续推进旧计划。

【普通推进规则】：
- 若不清楚任务情况，读取本地 readme.md 内容。
- 若尚未初始化，输出生成详尽 todo.md 的单一配置。
- 若历史里有用户问答结果，必须按用户选择继续推进，不要丢失用户决策。
- 若已有规划正在推进中，结合最新执行反馈输出下一步应执行的单一配置。
- 每完成一项，在todo.md中打勾。
- 若所有项已全部完成，输出 finish 配置。
请输出当前步骤的配置：`

// ==========================================
// 6. 多模态容错提取与原生工具协议装配
// ==========================================
function safeParseJson(str) {
  if (!str) return null;
  const clean = cleanLooseJson(str);
  try {
    return JSON.parse(clean);
  } catch (e) {
    try {
      const fixed = clean.replace(/:\s*"([^"\\]*(?:\\.[^"\\]*)*)"/gs, (_, p1) => {
        return ': "' + p1.replace(/\r?\n/g, '\\n') + '"';
      });
      return JSON.parse(fixed);
    } catch (e2) {
      return null;
    }
  }
}

function cleanLooseJson(str) {
  return str.replace(/,\s*([}\]])/g, '$1').replace(/\r\n/g, '\n').trim();
}

function scanBalancedJsonObject(text) {
  let depth = 0;
  let inStr = false;
  let quoteChar = '';
  let escape = false;
  let start = -1;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inStr) {
      if (escape) escape = false;
      else if (ch === '\\') escape = true;
      else if (ch === quoteChar) inStr = false;
      continue;
    }
    if (ch === '"' || ch === "'") {
      inStr = true;
      quoteChar = ch;
    } else if (ch === '{') {
      if (depth === 0) start = i;
      depth++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0 && start !== -1) {
        const slice = text.slice(start, i + 1);
        const obj = safeParseJson(slice);
        if (obj) return obj;
        start = -1;
      }
    }
  }
  return null;
}

function assertNonEmptyUpstreamText(text, scene = '调度') {
  const clean = String(text || '').trim();

  if (!clean) {
    throw new Error(`上游 LLM 返回空内容，${scene}失败，不能伪造为任务已完成`);
  }

  return clean;
}

function extractActionAndThought(rawText) {
  if (!rawText || typeof rawText !== 'string') return null;
  let thought = '';
  let action = '';
  let params = {};

  const thoughtMatch = rawText.match(/【思考】[：:]\s*([\s\S]*?)(?=【调度动作】|```json|```|<tool_call>|$)/i);
  if (thoughtMatch) {
    thought = thoughtMatch[1].trim();
  }

  const actionMatch = rawText.match(/【调度动作】[：:]\s*([a-zA-Z0-9_]+)/i);
  if (actionMatch) {
    action = actionMatch[1].trim();
  }

  const mdMatch = rawText.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  let parsedJson = null;
  if (mdMatch) {
    parsedJson = safeParseJson(mdMatch[1]);
  }
  if (!parsedJson) {
    parsedJson = scanBalancedJsonObject(rawText);
  }

  if (parsedJson && typeof parsedJson === 'object') {
    if (parsedJson.action) {
      action = parsedJson.action;
      thought = parsedJson.step_thought || parsedJson.thought || thought;
      params = parsedJson.params || parsedJson.arguments || parsedJson;
      if (params.action) {
        const { action: _a, step_thought: _st, thought: _t, ...rest } = params;
        params = rest;
      }
    } else if (action) {
      params = parsedJson;
    } else if (parsedJson.file_path && parsedJson.content !== undefined) {
      action = 'fs_write';
      params = parsedJson;
    } else if (parsedJson.command) {
      action = 'shell_exec';
      params = parsedJson;
    } else if (parsedJson.file_path && parsedJson.old_string !== undefined) {
      action = 'fs_replace';
      params = parsedJson;
    } else if (parsedJson.file_path) {
      action = 'fs_read';
      params = parsedJson;
    }
  }

  if (!thought && !action && !Object.keys(params).length) {
    return null;
  }

  return {
    thought: thought || '执行当前流水线步骤...',
    action: action || 'finish',
    params: params || {}
  };
}

// ==========================================
// 7. 上游通信器
// ==========================================
async function* readSSE(response) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || '';

      let dataLines = [];
      for (const line of lines) {
        if (line.startsWith('data:')) {
          dataLines.push(line.slice(5).trim());
        } else if (line === '' && dataLines.length > 0) {
          yield dataLines.join('\n');
          dataLines = [];
        }
      }
    }
    if (buffer.trim()) yield buffer.replace(/^data:\s*/, '');
  } finally {
    reader.releaseLock();
  }
}

async function fetchUpstreamStream(targetBase, apiKey, model, prompt, onThinkingChunk, signal) {
  const headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Authorization': `Bearer ${apiKey}`,
    'x-api-key': apiKey,
    'anthropic-version': '2023-06-01',
    'Accept': 'text/event-stream, application/json'
  };

  try {
    const res = await fetch(`${targetBase}/v1/messages`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model: model || 'claude-3-7-sonnet-20250219',
        max_tokens: 8192,
        messages: [{ role: 'user', content: prompt }],
        stream: true
      }),
      signal
    });

    if (res.ok) {
      let fullText = '';
      let thinkingText = '';
      for await (const chunk of readSSE(res)) {
        if (!chunk || chunk === '[DONE]') continue;
        try {
          const payload = JSON.parse(chunk);
          if (payload.type === 'content_block_delta') {
            if (payload.delta?.type === 'thinking_delta' && payload.delta.thinking) {
              thinkingText += payload.delta.thinking;
              if (onThinkingChunk) onThinkingChunk(payload.delta.thinking);
            } else if (payload.delta?.type === 'text_delta' && payload.delta.text) {
              fullText += payload.delta.text;
            }
          }
        } catch {}
      }
      return { text: fullText, thinking: thinkingText };
    }
  } catch (e) {}

  const chatRes = await fetch(`${targetBase}/v1/chat/completions`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      model: model || 'claude-3-7-sonnet-20250219',
      messages: [{ role: 'user', content: prompt }],
      stream: true
    }),
    signal
  });

  if (!chatRes.ok) {
    const errText = await chatRes.text();
    throw new Error(`上游调用完全失败: HTTP ${chatRes.status} - ${errText}`);
  }

  let fullText = '';
  let thinkingText = '';
  let inThinkTag = false;
  const tagOpen = '<' + 'think>';
  const tagClose = '<' + '/think>';

  for await (const chunk of readSSE(chatRes)) {
    if (!chunk || chunk === '[DONE]') continue;
    try {
      const payload = JSON.parse(chunk);
      const delta = payload.choices?.[0]?.delta;
      if (delta) {
        const think = delta.reasoning_content || delta.reasoning || '';
        if (think) {
          thinkingText += think;
          if (onThinkingChunk) onThinkingChunk(think);
        }

        if (delta.content) {
          let piece = delta.content;
          while (piece.length > 0) {
            if (!inThinkTag) {
              const start = piece.indexOf(tagOpen);
              if (start !== -1) {
                fullText += piece.slice(0, start);
                inThinkTag = true;
                piece = piece.slice(start + tagOpen.length);
              } else {
                fullText += piece;
                piece = '';
              }
            } else {
              const end = piece.indexOf(tagClose);
              if (end !== -1) {
                const tPiece = piece.slice(0, end);
                thinkingText += tPiece;
                if (onThinkingChunk) onThinkingChunk(tPiece);
                inThinkTag = false;
                piece = piece.slice(end + tagClose.length);
              } else {
                thinkingText += piece;
                if (onThinkingChunk) onThinkingChunk(piece);
                piece = '';
              }
            }
          }
        }
      }
    } catch {}
  }
  return { text: fullText, thinking: thinkingText };
}

// ==========================================
// 8. 路由: /v1/models 与 /v1/messages/count_tokens
// ==========================================

// ==========================================
// Token 估算工具：用于提前触发 compact + 限制代理二次拼接 prompt
// ==========================================
const MAX_PROXY_PROMPT_CHARS = Number(process.env.MAX_PROXY_PROMPT_CHARS || 120000);
const MAX_HISTORY_CHARS = Number(process.env.MAX_HISTORY_CHARS || 70000);
const MAX_GLOBAL_TASK_CHARS = Number(process.env.MAX_GLOBAL_TASK_CHARS || 20000);

function estimateTokensFromText(text = '') {
  const str = typeof text === 'string' ? text : JSON.stringify(text || {});
  // 温和估算
  return Math.max(1, Math.ceil(str.length / 4));
}

function estimateTokensFromPayload(payload = {}) {
  return estimateTokensFromText(JSON.stringify(payload || {}));
}

function hardLimitText(text = '', maxChars = 10000, label = '内容') {
  const str = String(text || '');
  if (str.length <= maxChars) return str;

  const head = Math.floor(maxChars * 0.45);
  const tail = Math.floor(maxChars * 0.45);

  return `${str.slice(0, head)}

...[${label}过长，已强制截断 ${str.length - head - tail} 字符，避免 Prompt is too long]...

${str.slice(-tail)}`;
}

function limitProxyPrompt(prompt) {
  return hardLimitText(prompt, MAX_PROXY_PROMPT_CHARS, '代理发送给上游的Prompt');
}

app.get(/(.*)\/v1\/models$/, async (req, res) => {
  const { upstreamBase } = parseTargetUrl(req);
  logger.debug('拉取模型列表', { 目标上游: upstreamBase });

  try {
    const authHeader = req.headers['authorization'] || `Bearer ${req.headers['x-api-key'] || ''}`;
    const upstreamRes = await fetch(`${upstreamBase}/v1/models`, {
      headers: {
        'Authorization': authHeader,
        'x-api-key': req.headers['x-api-key'] || ''
      },
      signal: AbortSignal.timeout(10000)
    });
    if (upstreamRes.ok) return res.json(await upstreamRes.json());
  } catch (e) {}

  res.json({
    object: 'list',
    data: [
      { id: 'claude-3-7-sonnet-20250219', object: 'model' },
      { id: 'claude-3-5-sonnet-20241022', object: 'model' }
    ]
  });
});

// 关键修改：不要 Math.min(..., 10000)，否则 CC 以为上下文永远不大，不会自动 compact
app.post(/(.*)\/v1\/messages\/count_tokens$/, (req, res) => {
  const { messages } = req.body || {};
  const { globalTask, historyLogsText } = parseConversation(messages || []);
  const proxyPrompt = limitProxyPrompt(buildPrompt(globalTask, historyLogsText));

  // 返回代理真正可能发送给上游的 prompt 估算，而不是只估原始 req.body
  const inputTokens = estimateTokensFromText(proxyPrompt);

  res.json({ input_tokens: inputTokens });
});

function isCompactionRequest(messages) {
  if (!Array.isArray(messages) || messages.length === 0) return false;
  const lastMsg = messages[messages.length - 1];
  const content = typeof lastMsg.content === 'string'
    ? lastMsg.content
    : (Array.isArray(lastMsg.content) ? lastMsg.content.map(c => c.text || '').join(' ') : '');
  return /summary of the conversation so far|summarize the conversation|compact|create a detailed summary/i.test(content);
}

// ==========================================
// 9. 核心路由: POST */v1/messages
// 修改点：
// 1. usage 不再固定低报
// 2. 不向 Claude Code 回传上游 thinking
// 3. textContent 继续保持极短
// ==========================================
app.post(/(.*)\/v1\/messages$/, async (req, res) => {
  const startTime = Date.now();
  const { upstreamBase } = parseTargetUrl(req);
  const apiKey = req.headers['x-api-key'] || (req.headers['authorization'] || '').replace('Bearer ', '');
  const { model, messages, stream } = req.body;

  const upstreamAbort = new AbortController();
  let finished = false;

  const abortUpstream = () => {
    if (!finished && !upstreamAbort.signal.aborted) {
      upstreamAbort.abort();
      logger.debug('下游连接断开，已取消上游请求', {
        目标上游: upstreamBase
      });
    }
  };

  // req.on('close', abortUpstream);
  res.on('close', abortUpstream);

  const isCompacting = isCompactionRequest(messages);
  const { globalTask, historyLogsText, latestTurnInput } = parseConversation(messages || []);

  const rawRequestTokens = estimateTokensFromPayload(req.body || {});

  logger.debug('收到 Claude Code 调度请求', {
    '目标上游': upstreamBase,
    '模型': model,
    '压缩模式': isCompacting ? '是 (Compaction)' : '否',
    '原始请求估算Token': rawRequestTokens,
    '本次增量输入': latestTurnInput || '（初始启动任务）'
  });

  let prompt = '';
  if (isCompacting) {
    prompt = `请对以下任务流水线当前的历史进展提供一份结构化、简明扼要的摘要总结，包括：已完成的步骤、生成/修改的文件清单、关键错误/测试结果、用户问答选择、以及当前待推进的下一个阶段。注意：用户问答选择必须完整保留，不得改写含义。请直接给出总结文本：\n\n【全局任务】：${globalTask}\n\n【执行历史】：\n${historyLogsText}`;
  } else {
    prompt = buildPrompt(globalTask, historyLogsText);
  }

  const requestInputTokens = estimateTokensFromText(prompt);

  logger.debug('中介实际上游Prompt', {
    'Prompt字符数': prompt.length,
    '实际上游Prompt估算Token': requestInputTokens
  });

  prompt = limitProxyPrompt(prompt);

  const msgId = 'msg_' + crypto.randomBytes(12).toString('hex');
  let heartbeatTimer = null;
  let blockIndex = 0;

  const sendSSE = (ev, data) => {
    if (!res.writableEnded) {
      res.write(`event: ${ev}\ndata: ${JSON.stringify(data)}\n\n`);
    }
  };

  if (stream) {
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    sendSSE('message_start', {
      type: 'message_start',
      message: {
        id: msgId,
        type: 'message',
        role: 'assistant',
        model: model || 'claude-3-7-sonnet',
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: {
          input_tokens: requestInputTokens,
          output_tokens: 1
        }
      }
    });

    heartbeatTimer = setInterval(() => {
      if (!res.writableEnded) res.write(': keep-alive\n\n');
    }, 5000);

    // 关键：发真正 Anthropic SSE ping，不发注释
    // heartbeatTimer = setInterval(() => {
    //   if (!res.writableEnded) {
    //     sendSSE('ping', { type: 'ping' });
    //   }
    // }, 15000);
  }

  try {
    const { text: assistantText } = await fetchUpstreamStream(
      upstreamBase,
      apiKey,
      model,
      prompt,
      null,
      upstreamAbort.signal
    );
    
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    
    const safeAssistantText = assertNonEmptyUpstreamText(
      assistantText,
      isCompacting ? '历史压缩' : '任务调度'
    );
    
    let stopReason = 'end_turn';
    let textContent = '';
    let toolBlock = null;
    
    if (isCompacting) {
      textContent = safeAssistantText
        .replace(/【思考】[\s\S]*?(?=【调度动作】|$)/gi, '')
        .trim();
    
      if (!textContent) {
        throw new Error('上游 LLM 历史压缩结果为空');
      }
    
      stopReason = 'end_turn';
    } else {
      const parsedAction = extractActionAndThought(safeAssistantText);
    
      if (parsedAction && parsedAction.action && parsedAction.action !== 'finish') {
        const mappedTool = mapActionToClaudeCodeTool(parsedAction.action, parsedAction.params);
        stopReason = 'tool_use';
    
        const targetDesc = mappedTool.arguments?.file_path || mappedTool.arguments?.command || '';
        textContent = `调度 ${mappedTool.name}${targetDesc ? ' -> ' + targetDesc : ''}`.slice(0, 80);
    
        toolBlock = {
          type: 'tool_use',
          id: 'toolu_' + crypto.randomBytes(10).toString('hex'),
          name: mappedTool.name,
          input: mappedTool.arguments
        };
    
        logger.debug('成功装配 CC 原生工具调用', {
          '耗时': `${Date.now() - startTime}ms`,
          '下发原生工具': mappedTool.name,
          '参数大小': `${JSON.stringify(mappedTool.arguments).length} 字符`
        });
      } else if (parsedAction && parsedAction.action === 'finish') {
        textContent = parsedAction.params?.summary || '任务已完成。';
        stopReason = 'end_turn';
      } else {
        throw new Error(
          `上游 LLM 输出无法解析为有效调度动作，不能当作任务完成。原始输出：${safeAssistantText.slice(0, 500)}`
        );
      }
    }

    const outputTokens =
      estimateTokensFromText(textContent) +
      estimateTokensFromPayload(toolBlock || {}) +
      8;

    if (stream) {
      if (textContent) {
        sendSSE('content_block_start', {
          type: 'content_block_start',
          index: blockIndex,
          content_block: { type: 'text', text: '' }
        });
        sendSSE('content_block_delta', {
          type: 'content_block_delta',
          index: blockIndex,
          delta: { type: 'text_delta', text: textContent }
        });
        sendSSE('content_block_stop', {
          type: 'content_block_stop',
          index: blockIndex
        });
        blockIndex++;
      }

      if (toolBlock) {
        sendSSE('content_block_start', {
          type: 'content_block_start',
          index: blockIndex,
          content_block: {
            type: 'tool_use',
            id: toolBlock.id,
            name: toolBlock.name,
            input: {}
          }
        });
        sendSSE('content_block_delta', {
          type: 'content_block_delta',
          index: blockIndex,
          delta: {
            type: 'input_json_delta',
            partial_json: JSON.stringify(toolBlock.input)
          }
        });
        sendSSE('content_block_stop', {
          type: 'content_block_stop',
          index: blockIndex
        });
        blockIndex++;
      }

      sendSSE('message_delta', {
        type: 'message_delta',
        delta: {
          stop_reason: stopReason,
          stop_sequence: null
        },
        usage: {
          output_tokens: outputTokens
        }
      });
      sendSSE('message_stop', { type: 'message_stop' });

      finished = true;
      res.end();
    } else {
      finished = true;
      res.json({
        id: msgId,
        type: 'message',
        role: 'assistant',
        model,
        content: [
          ...(textContent ? [{ type: 'text', text: textContent }] : []),
          ...(toolBlock ? [toolBlock] : [])
        ],
        stop_reason: stopReason,
        stop_sequence: null,
        usage: {
          input_tokens: requestInputTokens,
          output_tokens: outputTokens
        }
      });
    }
  } catch (err) {
    if (heartbeatTimer) clearInterval(heartbeatTimer);

    if (upstreamAbort.signal.aborted) {
      logger.debug('v1/message上游请求已因下游断开而中止', {
        目标上游: upstreamBase
      });
      return;
    }

    logger.error('Claude Code 消息通道异常', err.message);

    finished = true;
    if (!res.headersSent) {
      res.status(500).json({ error: { message: err.message } });
    } else {
      res.end();
    }
  } finally {
    logger.debug('v1/message 下游断开!!!');
    // req.off?.('close', abortUpstream);
    res.off?.('close', abortUpstream);
  }
});

// ==========================================
// 10. OpenWebUI / OpenAI 兼容通道
// 修改点：
// 1. 不再把 thinking/reasoning_content 返回给客户端
// 2. tool_calls 场景下 textContent 不使用 parsedAction.thought
// 3. usage 使用真实估算
// ==========================================
app.post(/(.*)\/v1\/chat\/completions$/, async (req, res) => {
  const { upstreamBase } = parseTargetUrl(req);
  const apiKey = (req.headers['authorization'] || '').replace('Bearer ', '') || req.headers['x-api-key'];
  const { model, messages, stream } = req.body;

  const upstreamAbort = new AbortController();
  let finished = false;

  const abortUpstream = () => {
    if (!finished && !upstreamAbort.signal.aborted) {
      upstreamAbort.abort();
      logger.debug('下游连接断开，已取消上游请求', {
        目标上游: upstreamBase
      });
    }
  };

  // req.on('close', abortUpstream);
  res.on('close', abortUpstream);

  const { globalTask, historyLogsText, latestTurnInput } = parseConversation(messages || []);

  const rawRequestTokens = estimateTokensFromPayload(req.body || {});

  logger.debug('收到 OpenAI/ChatCompletions 调度请求', {
    目标上游: upstreamBase,
    原始请求估算Token: rawRequestTokens,
    本次增量输入: latestTurnInput || '（初始启动任务）'
  });

  let heartbeatTimer = null;

  if (stream) {
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    // 关键：OpenAI 兼容流发空 delta，不发 SSE 注释
    heartbeatTimer = setInterval(() => {
      if (!res.writableEnded) {
        res.write(`data: ${JSON.stringify({
          id: 'chatcmpl-keepalive',
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model,
          choices: [{
            index: 0,
            delta: {}
          }]
        })}\n\n`);
      }
    }, 5000);
  }

  try {
    const prompt = limitProxyPrompt(buildPrompt(globalTask, historyLogsText));

    const requestInputTokens = estimateTokensFromText(prompt);

    logger.debug('中介实际上游Prompt', {
      Prompt字符数: prompt.length,
      实际上游Prompt估算Token: requestInputTokens
    });

    const { text: assistantText } = await fetchUpstreamStream(
      upstreamBase,
      apiKey,
      model,
      prompt,
      null,
      upstreamAbort.signal
    );
    
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    
    const safeAssistantText = assertNonEmptyUpstreamText(assistantText, '任务调度');
    const parsedAction = extractActionAndThought(safeAssistantText);
    
    const callId = 'call_' + crypto.randomBytes(8).toString('hex');
    let toolCalls = null;
    let finishReason = 'stop';
    let textContent = '';
    
    if (parsedAction && parsedAction.action && parsedAction.action !== 'finish') {
      const mappedTool = mapActionToClaudeCodeTool(parsedAction.action, parsedAction.params);
      finishReason = 'tool_calls';
    
      const targetDesc = mappedTool.arguments?.file_path || mappedTool.arguments?.command || '';
      textContent = `调度 ${mappedTool.name}${targetDesc ? ' -> ' + targetDesc : ''}`.slice(0, 80);
    
      toolCalls = [{
        index: 0,
        id: callId,
        type: 'function',
        function: {
          name: mappedTool.name,
          arguments: JSON.stringify(mappedTool.arguments)
        }
      }];
    } else if (parsedAction && parsedAction.action === 'finish') {
      textContent = parsedAction.params?.summary || '任务已完成。';
      finishReason = 'stop';
    } else {
      throw new Error(
        `上游 LLM 输出无法解析为有效调度动作，不能当作任务完成。原始输出：${safeAssistantText.slice(0, 500)}`
      );
    }

    const outputTokens =
      estimateTokensFromText(textContent) +
      estimateTokensFromPayload(toolCalls || {}) +
      8;

    if (stream) {
      if (textContent) {
        res.write(`data: ${JSON.stringify({
          id: 'chatcmpl-1',
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model,
          choices: [{
            delta: {
              content: textContent
            },
            index: 0
          }]
        })}\n\n`);
      }

      if (toolCalls) {
        res.write(`data: ${JSON.stringify({
          id: 'chatcmpl-1',
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model,
          choices: [{
            delta: {
              tool_calls: toolCalls
            },
            index: 0
          }]
        })}\n\n`);
      }

      res.write(`data: ${JSON.stringify({
        id: 'chatcmpl-1',
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model,
        choices: [{
          delta: {},
          finish_reason: finishReason,
          index: 0
        }],
        usage: {
          prompt_tokens: requestInputTokens,
          completion_tokens: outputTokens,
          total_tokens: requestInputTokens + outputTokens
        }
      })}\n\n`);

      res.write('data: [DONE]\n\n');

      finished = true;
      res.end();
    } else {
      finished = true;
      res.json({
        id: 'chatcmpl-' + crypto.randomBytes(8).toString('hex'),
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model,
        choices: [{
          message: {
            role: 'assistant',
            content: textContent,
            ...(toolCalls ? { tool_calls: toolCalls } : {})
          },
          finish_reason: finishReason,
          index: 0
        }],
        usage: {
          prompt_tokens: requestInputTokens,
          completion_tokens: outputTokens,
          total_tokens: requestInputTokens + outputTokens
        }
      });
    }
  } catch (err) {
    if (heartbeatTimer) clearInterval(heartbeatTimer);

    if (upstreamAbort.signal.aborted) {
      logger.debug('上游请求已因下游断开而中止', {
        目标上游: upstreamBase
      });
      return;
    }

    logger.error('ChatCompletions 消息通道异常', err.message);

    finished = true;
    if (!res.headersSent) {
      res.status(500).json({ error: { message: err.message } });
    } else {
      res.end();
    }
  } finally {
    logger.debug('/v1/chat/completions下游断开!!!');
    // req.off?.('close', abortUpstream);
    res.off?.('close', abortUpstream);
  }
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n======================================================`);
  console.log(` CC 已就绪 (端口: ${PORT})`);
  console.log(`======================================================\n`);
});

server.requestTimeout = 2400000;
server.headersTimeout = 2400000;
server.keepAliveTimeout = 120000;
