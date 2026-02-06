---
name: pr-review
description:
  Reviews pull requests for the ide-sidecar project. Use when reviewing PRs, doing self-review
  before sharing with the team, or when user mentions "review PR", "help with PR", "review changes",
  "self-review", "review local changes", or "check my PR". Provides generic PR Review skill with
  project-specific patterns and tech stack-related requirements.
tools: [Read, Bash, Grep, Glob, Task]
---

# PR Review Skill

Review pull requests for the ide-sidecar project, helping reviewers understand changes and authors
self-review before formal team review. Focuses on Java code quality, testing, documentation,
native build considerations, and other project-specific patterns and requirements.

## Two Review Modes

### Self-Review Mode (for PR authors)

Use when: Author wants to check their changes before sharing with the team. Typically used with PRs
in draft mode or against local changes before pushing them to GitHub.

Goals:

- Catch issues early before formal review
- Verify critical requirements are met
- Ensure proper test coverage exists
- Validate architectural patterns are followed

### Formal Review Mode (for reviewers)

Use when: Reviewer needs to understand and evaluate a PR from another team member. Typically used
with PRs that are ready for review and have been shared with the team.

Goals:

- Quickly understand the scope and purpose of changes
- Identify potential issues or concerns
- Provide constructive feedback
- Verify checklist items are addressed

## Review Process

### Step 1: Gather Information

**For local changes:**

```bash
# See what files changed
git diff --name-only HEAD~1  # or compare against main
git diff main --name-only

# See the actual changes
git diff main --stat
git diff main
```

**For GitHub PRs:**

```bash
# Get PR details if PR number or URL provided
gh pr view <PR_NUMBER> --json number,title,body,author,baseRefName,headRefName,additions,deletions,changedFiles,state,reviewDecision

# If PR number or URL not provided, try current branch
gh pr view --json number,title,body,author,baseRefName,headRefName,additions,deletions,changedFiles,state,reviewDecision

# Get the diff
gh pr diff <PR_NUMBER>

# Get existing review comments if any
gh pr view <PR_NUMBER> --json reviews,comments
```

### Step 2: Understand the Context

1. **Read the PR description** - Verify it clearly explains:
  - What changes were made
  - Why the changes were needed
  - How to test the changes

2. **Identify changed files** - Categorize them:
  - Source code (`src/main/java/...`)
  - Tests (`src/test/java/...`)
  - Configuration (`application.yml`, `pom.xml`)
  - Documentation (`*.md`)
  - Configuration of GraalVM (`src/main/resources/META-INF/native-image/...`)
  - Static resources (`src/main/resources/...`)
  - OpenAPI/GraphQL schema files (`src/generated/resources/...`)
  - CI configuration (`.semaphore/...`)

3. **Summarize the PR** - Provide a brief summary of:
  - The purpose and scope of the changes
  - Key files and components affected
  - Any architectural decisions made

### Step 3: Review Against Project Guidelines

Apply the ide-sidecar code review standards:

#### Focus Areas

| Area | What to Check                                                                                                                                                                      |
|------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Single Responsibility** | Do classes and methods maintain focused, single purposes?                                                                                                                          |
| **Security** | Are there any security concerns (e.g., hardcoded secrets, command injection, OWASP Top 10)?                                                                                        |
| **GraalVM Compatibility** | Are there any issues that would prevent successful native compilation (e.g., missing `@RegisterForReflection`, native library usage, usage of dynamic loading of Java services)?   |
| **Testing Coverage** | Does new functionality include appropriate tests? Favor unit over integration tests, but use integration tests where necessary. We're aiming for a test coverage of 80% or higher. |
| **Test Cases** | Are features reasonably tested? Are edge cases covered?                                                                                                                            |
| **Error Handling** | Do catch blocks NEVER swallow exceptions? Are exceptions logged at ERROR level?                                                                                                    |
| **API Changes** | Are user-facing API endpoint changes versioned if possible? Do API changes follow the [API Design Guide](https://github.com/confluentinc/api/blob/master/DESIGN_GUIDE.md)?         |
| **Documentation** | Are code changes documented in Javadocs?                                                                                                                                           |
| **Complexity** | Is unnecessary complexity avoided to keep code maintainable and testable?                                                                                                          |

#### Javadoc and Comment Preservation

- **Never suggest deleting existing Javadocs or comments** unless they contain significant errors
- Javadocs provide valuable context about business logic, architectural decisions, and edge cases
- When suggesting improvements, enhance Javadocs rather than removing them

#### Style Guidelines

- Follow the **Google Java style guide**
- Class, method, and variable names should be **precise and short** for readability
- Check for **duplicated code** introduced by the PR
- Focus on substantive issues: logic, architecture, testing - avoid nitpicking formatting

#### Review Checklist

- [ ] PR description is clear and complete
- [ ] Applicable checklist items in PR template are completed
- [ ] Author has click-tested changes against the **native executable** (critical for GraalVM)

### Step 4: Check for Common Issues

Scan the diff for these patterns:

1. **Missing `@RegisterForReflection`** - Required for DTOs/records accessed via reflection in native builds
2. **Exception swallowing** - Empty catch blocks or catches that don't log at ERROR level
3. **Resource leaks** - Unclosed streams, connections, or Kafka clients
4. **Thread safety** - Shared mutable state without synchronization
5. **Breaking API changes** - REST/GraphQL endpoint modifications without versioning
6. **Security concerns** - Hardcoded secrets, command injection, OWASP Top 10 vulnerabilities
7. **Null safety** - Missing null checks on external inputs

### Step 5: Provide Structured Feedback

Output the review in this format:

---

## PR Review: #{number} - {title}

**Author:** {author}
**Branch:** {headRefName} → {baseRefName}
**Changes:** +{additions} / -{deletions} across {changedFiles} files

### Summary

{2-3 sentence summary of what the PR does and why}

### Changed Components

- {List key files/components affected with brief descriptions}

### Findings

#### Issues (Must Fix)
{List blocking issues with `file:line` references, or "None found"}

#### Suggestions (Consider)
{List non-blocking suggestions for improvement, or "None"}

#### Positive Observations
{Call out good patterns, thorough testing, or well-written code}

### Test Coverage Assessment

- **New tests added:** {Yes/No - list test files}
- **Coverage gaps:** {Identify untested paths or edge cases}
- **Test type balance:** {Unit vs Integration test ratio assessment}

### Native Build Considerations

{Any concerns about GraalVM native compilation: missing reflection registration, native library usage, usage of Java features not available in GraalVM (e.g., dynamic service loading), etc.}

### Recommendation

**{APPROVE / REQUEST CHANGES / NEEDS DISCUSSION}**

{Brief rationale for the recommendation}
