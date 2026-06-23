---
name: doc-qa
description: Interactive Reddit-style threaded Q&A within markdown or text documents using nested blockquote threading. Use when the user asks questions about document content inline, discusses design docs, or when a markdown file contains **Q:** blockquote entries without corresponding **A:** answers. Also use when the user says "doc qa", "ask about this doc", or "threaded Q&A".
---

# Document Q&A — Reddit-Style Threaded Discussion

## Trigger

Activate this skill when:

- A markdown/text file contains `**Q:**` in blockquotes without a matching `**A:**` at the next depth
- The user asks to discuss, question, or annotate a document inline
- The user references this skill by name

## Threading Convention

Questions and answers are threaded using blockquote nesting (`>`). Each interaction increases depth by one level.

### Nesting Rules

1. **User question** — one `>` level deeper than the content it references
2. **Agent answer** — one `>` level deeper than the question
3. **Follow-up question** — one `>` level deeper than the answer it's about
4. **Sibling question** — same `>` level as an existing question (parallel thread)

### Format

- Questions: `**Q:** question text`
- Answers: `**A:** answer text`
- Separate each Q/A block with a blank line using matching `>` prefix for blockquote continuity

## Example

```markdown
Document content about distributed consensus...

> **Q:** Why Raft over Paxos here?
>
>> **A:** Raft is easier to implement correctly. It decomposes consensus into leader election, log replication, and safety — each understandable independently. Paxos merges these concerns.
>>
>>> **Q:** What about Multi-Paxos performance?
>>>
>>>> **A:** Multi-Paxos amortizes leader election cost and can match Raft throughput. The choice here is operability, not raw performance.
>>>
>>> **Q:** Any production systems still using Paxos?
>>>
>>>> **A:** Yes — Google's Chubby and Spanner use Paxos variants internally.
>>
>> **Q:** Does Raft handle Byzantine faults?
>>
>>> **A:** No. Raft assumes crash-fault only. For Byzantine tolerance you'd need PBFT or HotStuff.

More document content continues...
```

## Workflow

1. **Read** the full document (or relevant section around the question) for context
2. **Locate** every unanswered `**Q:**` — identified by having no corresponding `**A:**` at the next depth
3. **Count** the `>` depth of each question
4. **Write** each answer at question depth + 1, prefixed with `**A:**`
5. Keep answers **concise but thorough** — use bullet points, code blocks, and bold for key terms
6. If the answer references other parts of the document, cite the section heading

## Multi-paragraph Answers

Continue at the same `>` depth with blank blockquote lines between paragraphs:

```markdown
>> **A:** First paragraph of the answer.
>>
>> Second paragraph continues at same depth.
```

## Code Blocks Inside Answers

Fence the code block at the matching blockquote depth:

```markdown
>> **A:** Here's the retry logic:
>>
>> ```python
>> for attempt in range(max_retries):
>>     try:
>>         return call_service()
>>     except Timeout:
>>         backoff(attempt)
>> ```
```

## Rules

- **Be terse by default.** Give the shortest correct answer. Only elaborate when the question explicitly asks for detail or the topic genuinely requires it.
- If the document or your knowledge doesn't contain enough information to answer confidently, say so plainly (e.g., "Not enough context in this doc to answer — would need X").
- NEVER modify existing document content outside the Q&A thread
- NEVER remove or rewrite the user's questions
- If a question is ambiguous, answer your best interpretation and add a clarifying note
- When the user places multiple unanswered `**Q:**` entries, answer ALL of them
- Answers should draw from the document's own context first, then general knowledge
