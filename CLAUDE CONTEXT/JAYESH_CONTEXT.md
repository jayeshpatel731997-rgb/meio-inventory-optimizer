# JAYESH PATEL — MASTER CLAUDE CONTEXT FILE
**Drop this in C:\Users\JAYESHAI\CLAUDE CONTEXT\ and reference at the start of every session.**

---

## WHO I AM

- **Name:** Jayesh Patel
- **Location:** Chicago, IL
- **Degree:** M.S. Industrial Technology & Operations, IIT Chicago — graduating December 2025
- **GPA:** 4.0
- **Visa:** Require H1B sponsorship — only target companies that sponsor
- **Email:** jayeshpatel731997@gmail.com
- **LinkedIn:** linkedin.com/in/jayeshpatel73

**Background in one paragraph:**
6 years real supply chain experience (JSK Industries — food equipment manufacturing; Shreeji Healthcare — pharmaceutical distribution) in India. Inventory management, procurement, supplier coordination, BOM-level planning. Built a production-grade Supplier Intelligence Platform in 2 weeks with zero prior Python knowledge. That project uses Bayesian risk scoring, SIR epidemic propagation, Monte Carlo VaR/CVaR (5,000+ iterations), graph centrality (PageRank, Betweenness, articulation points), TCO analysis, and Claude API integration for news classification. Deployed live on Streamlit.

---

## MY TECHNICAL LEVEL (Be Honest)

- **Python:** Beginner-intermediate. Can read and understand code. Can modify existing code. Cannot write complex logic from scratch without help.
- **SQL:** Basic — still building
- **Tools I use daily:** VS Code, Cursor, GitHub, Streamlit, Claude, ChatGPT Plus, Perplexity Plus, Codex
- **How I learn:** Build first, understand second. Claude writes it, I run it, I ask why, I learn by touching real code.
- **Pareto rule:** Learning Pandas + NumPy only right now — not trying to master everything at once.

---

## ACTIVE PROJECTS

### Project 1: Supplier Intelligence Platform ✅ BUILT
- Live on Streamlit, code on GitHub
- Bayesian posterior P(disruption) from 6 evidence signals
- SIR epidemic propagation model for disruption cascade simulation
- Monte Carlo 5,000+ iterations — VaR, CVaR, Expected Loss, P90
- Graph-theoretic network analysis — PageRank, Betweenness, SPOF detection
- TCO analysis with COPQ, delivery variability costs, switching costs
- Stack: Python, Streamlit, Plotly, NumPy, SciPy, NetworkX, SQLite

### Project 2: Multi-Echelon Inventory Optimizer (MEIO) 🔨 IN PROGRESS
- **Timeline:** 4 weeks @ 20 hours/week
- **Target companies:** Amazon, Walmart, Tesla, Microsoft, consulting firms
- **Stack:** PostgreSQL, Python (pandas/numpy/scipy/stockpyl/pulp), Streamlit, Excel
- **Local folder:** C:\Users\JAYESHAI\SUPPLIER PORTFOLIO PROJECT\MEIO\
- **What it builds:**
  - PostgreSQL database with 7 ERP-like tables (messy data)
  - SQL data pipeline (schema → ingest → cleaning → marts)
  - MEIO optimization (safety stock + base-stock levels by location)
  - LP allocation model (initial placement + transfer recommendations)
  - Monte Carlo simulation (5,000 iterations for service level validation)
  - Cost-to-serve calculator (freight + warehouse + handling + carrying)
  - Streamlit app (5 pages: dashboard, data quality, policy, scenarios, actions)
  - Excel export pack (planning-ready CSVs)
  - 2-3 page case study PDF (Amazon/Tesla/Walmart versions)
  - GitHub repo with clean documentation
- **What recruiters must think in 30 seconds:**
  - "He can handle messy ERP data with SQL"
  - "He speaks service level / OTIF / working capital / cost-to-serve"
  - "He outputs ACTIONS (move inventory, change policies), not just analysis"
  - "This could integrate with SAP IBP / Kinaxis / Blue Yonder"

---

## TARGET COMPANIES — H1B SPONSORS ONLY

**Tier 1:** Tesla, Amazon, Meta, Apple, Google, Microsoft
**Tier 2:** Rivian, SpaceX, Joby Aviation, Symbotic, Machina Labs
**Tier 3:** Accenture Supply Chain, Deloitte SCM Practice, Oliver Wyman, Kearney, McKinsey Operations

**Rule:** Never suggest startups under 100 employees — they rarely sponsor H1B.

---

## HOW TO WORK WITH ME — NON-NEGOTIABLE RULES

### Rule #1 — NEVER Use Paid APIs Without Asking
Before using ANY paid API (OpenAI, Anthropic, NewsAPI, paid data sources) — STOP and ask:
*"This will use [X] API which may cost money. Do you want me to proceed?"*
Free APIs and Kaggle datasets are fine without asking. Paid = always ask first.

### Rule #2 — Ask Clarifying Questions Before Doing Anything
Default behavior: Ask 2-3 clarifying questions BEFORE writing any code or creating files.
Only skip questions if: task is crystal clear, I say "just do it" or "execute", or it's a small edit.

### Rule #3 — Response Style
- Short answer for simple questions. Structured response for complex tasks.
- Never add filler or end with "Let me know if you have questions!"
- Build files for outputs >20 lines — don't dump walls of text in chat
- Bold the most important line in each section
- Always explain WHY you made a technical decision, not just what

### Rule #4 — Push Back When I'm Wrong
Say it directly: *"I'd push back on this — here's why: [reason]"*
Push back when I'm adding complexity that doesn't help the portfolio signal, or using paid tools when free equivalents exist.

### Rule #5 — Pareto Filter on Every Suggestion
Before recommending anything: "Does this make the project more real and impressive, or does it just add complexity?"

---

## MY BRAND VOICE

**Sounds RIGHT:**
- Direct and specific: "I built a Bayesian supplier risk platform. It identifies $13.3M in at-risk spend."
- Numbers over adjectives: Not "significantly improved" — say "reduced by 22%"
- Real over polished: I'd rather sound like a person who built something than someone who wrote about it.

**Sounds WRONG (never write like this):**
- "Synergize cross-functional stakeholder alignment to leverage supply chain efficiencies"
- "Results-oriented professional with demonstrated track record of excellence"
- "I just built a small project as part of my coursework"

**Writing principle:** Lead with the result, follow with the method.

---

## MY FOLDER STRUCTURE

```
C:\Users\JAYESHAI\
├── CLAUDE CONTEXT\          ← This file lives here
├── SUPPLIER PORTFOLIO PROJECT\
│   └── MEIO\                ← Active project folder for Cowork
├── data\
└── src\
```

**Project folder standard (every project):**
```
/project-name/
├── /data/raw/        ← original data, never modify
├── /data/processed/  ← cleaned data
├── /src/             ← Python scripts
├── /sql/             ← SQL files
├── /outputs/         ← final deliverables
├── app.py            ← main Streamlit app
├── requirements.txt
└── README.md
```

---

## TOOLS I'M USING ON MEIO

| Tool | Role |
|------|------|
| Claude (chat) | Architecture, code generation, debugging, documents |
| Cowork | Folder setup, file organization, CSV generation, document drafts |
| Codex (VS Code) | Autocomplete while coding |
| Perplexity Plus | Research (Graves & Willems, SAP IBP specs) |
| ChatGPT Plus | Second opinion on code |

**Cowork folder:** `C:\Users\JAYESHAI\SUPPLIER PORTFOLIO PROJECT\MEIO\`
**Cowork rule:** Only give it access to the MEIO folder — never root directory.

---

## SUCCESS DEFINITION

In 6 months:
1. Get noticed by recruiters BEFORE applying — through GitHub, LinkedIn, live demos
2. Build 2-3 more portfolio projects at the same caliber as the supplier platform
3. Land a role with H1B sponsorship at a company sophisticated enough to appreciate what I built
4. Make my experience section irrelevant — projects so strong interviewers don't care about job titles

**The platform is the real signal. Not the resume.**
