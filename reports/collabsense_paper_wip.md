# CollabSense: Evaluating Enterprise AI Coaching Models on Open Source Collaboration Data

**Status:** Work in Progress  
**Project:** ClarityLoop / CollabSense  
**Focus:** AI Sentiment Analysis, Behavioral Coaching, and Graph Density in Open Source

---

## 1. Abstract
> *Brief overview of the project. Highlight the successful deployment of the ClarityLoop engine on public open-source data. Emphasize the model's high accuracy in mapping sentiment and identifying positive "Strengths." Summarize how combining an "Individual Signal" (medium) prompt with engineered "Artificial Density" successfully allowed the engine to generate accurate "Growth Opportunities."*

## 2. Introduction
## 2. Introduction & Objective

The ClarityLoop AI engine is designed to function as an automated behavioral coach within enterprise environments. By analyzing daily workplace communications - such as peer-to-peer feedback, code reviews, and direct messaging - the engine identifies a team "Sentiment Score", highlights individual "Strengths", and surfaces actionable "Growth Opportunities" (constructive coaching moments) for employees. 

Historically, tools of this nature are optimized for the dense, hierarchical communication topologies typical of traditional corporate teams. The primary objective of this research is to evaluate the transferability of the ClarityLoop engine to the asynchronous, decentralized, and highly technical environment of Open Source Software (OSS) development. 

Specifically, this study uses public GitHub pull requests and issue comments from massive open-source repositories (`pandas-dev/pandas` and `kubernetes/kubernetes`) as a proxy for internal enterprise peer reviews. Open-source collaboration provides a massive, high-quality dataset of professional engineering interactions, making it an ideal stress-test for the AI's pattern-recognition capabilities.

This research seeks to answer three core questions:
1. **Sentiment Transferability:** Can an enterprise-tuned AI accurately interpret the tone and intent of open-source technical debates without hallucinating negativity or toxic behavior?
2. **Behavioral Recognition:** Can the engine distinguish between transactional code corrections (e.g., syntax fixes) and genuine behavioral coaching moments (e.g., communication style, collaboration, and leadership)?
3. **Structural Adaptability:** What prompt tuning and data-engineering techniques are required to overcome the inherent "sparsity" of open-source social graphs to successfully trigger the engine's Growth Opportunity algorithms?

## 3. Literature Review
> *Review existing research to establish the baseline of what has been done in this field.*

### 3.1 Sentiment Analysis in the Workplace
> *Research on how NLP and LLMs are used to track morale, tone, and toxicity in corporate communications (Slack, Teams, Email).*

### 3.2 Automated Feedback & Growth Signals
> *Explore existing tools or papers focused on automated performance reviews, behavioral coaching, or soft-skill extraction from text.*

### 3.3 The Gap: Enterprise vs. Open Source Topology
> *Research comparing the social graphs of traditional companies (dense, hierarchical) versus open-source projects (sparse, transactional, "drive-by" contributions). How does this structural difference affect AI analysis?*

## 4. Methodology & Data Engineering

The methodology for this study required building a custom Python data pipeline (`clarityloop/collabsense/src`) to extract, filter, and transform public GitHub interactions into a format that the ClarityLoop engine could process. 

Because the AI is designed to analyze internal corporate communications, feeding it raw open-source data directly would not yield accurate results. The pipeline was broken into three main stages: asynchronous data mining using the GitHub API, logic-based filtering to isolate valuable interactions, and data anonymization to simulate a corporate environment.

### 4.1 Dataset Selection & Anonymization

Two major open-source repositories were selected for this study: `pandas-dev/pandas` and `kubernetes/kubernetes`. These projects were chosen because of their massive scale, strict professional review standards, and highly active maintainer communities. These characteristics make them the closest available public proxy to a large, professional software engineering department.

**Filtering for Quality**
To ensure the AI was evaluating meaningful peer-to-peer feedback, strict filters were applied during the data ingestion phase:
* **Bot Removal:** Open-source repositories rely heavily on automation. The pipeline stripped out all interactions from known bots (e.g., accounts containing `[bot]`, `-bot`, or `bot-`) to guarantee the AI only analyzed human behavior.
* **Long-Term Contributor Focus:** To replicate the dynamics of permanent employees, we implemented tenure and activity filters. For the baseline Pandas dataset, we filtered for users who had been consistently active over extended periods (ranging from 6 months up to 5 consecutive years, depending on the test) and who met minimum thresholds for participating in pull requests and issues. This effectively removed one-time "drive-by" contributors, leaving only the core community.

**Anonymization and Workspace Simulation**
The ClarityLoop engine expects data formatted for a private enterprise organization. To adapt the public GitHub data, an anonymization module was built using the Python `Faker` library. 

This module systematically replaced all real GitHub usernames and public identities with synthetic profiles. It generated realistic fake names, corporate email addresses, user roles, and profile avatars. By mapping the filtered GitHub pull requests and comments to these synthetic profiles, we successfully generated clean, relational datasets (`users.csv`, `contexts.csv`, and `context_comments.csv`) that perfectly mirrored the structure of a private enterprise workspace. This allowed the engine to process the open-source data exactly as it would for a corporate client.

### 4.2 Engineering "Artificial Density" (K-Core)

The main challenge in applying enterprise AI to public repositories is the structural "sparsity" of open-source networks. While corporate teams interact with the same peers repeatedly (high density), open-source contributors often interact with dozens of strangers for single, transactional code merges. Initial tests on the Pandas repository revealed an interaction density of just 1.1%. Because the ClarityLoop engine relies on recognizing recurring behavioral patterns between peers, this sparse environment failed to trigger the Growth Opportunity algorithms.

**K-Core Decomposition & Strict Filtering**
To simulate an enterprise environment, the pipeline was upgraded to use **K-Core decomposition** on the Kubernetes repository. This graph algorithm identifies the "densest subgraph" of users who frequently interact with each other. 

However, because these maintainers act as public gatekeepers, capturing their entire comment history inadvertently pulled in thousands of external interactions, plummeting the network density to 0.2%. To fix this, we implemented a strict peer-to-peer allow-list: the pipeline deleted any comment where the sender or recipient was outside the identified core group. 

This filtering strategy successfully stripped away the transactional "noise" of the public repository. The resulting isolated datasets achieved enterprise-grade internal densities ranging from 35% to 75%, creating the exact topology required to stress-test the AI.

![Network Density Graph](./data/kubernetes-density-focused/network_density_graph.png)

![Core Team Graph](./data/kubernetes-density-focused/core_team_graph.png)

## 5. Baseline Success & The Sparsity Challenge
> *Focus on the initial `Pandas_LongTerm` run. Highlight the major successes first, then explain the structural hurdle.*

### 5.1 Validating Soft-Skill Extraction
> *Highlight the successful extraction of 120 Strengths from 1,162 comments (10.3% yield).*
> `[INSERT STRENGTHS VOLUME GRAPH HERE]`
> `[INSERT TOP 3 STRENGTHS TABLE HERE]`

### 5.2 The Missing Growth Opportunities
> *Explain that the lack of Growth Opportunities (0 yield) was not an AI comprehension failure, but a structural mismatch. The 1.1% network density prevented the AI from seeing the "recurring patterns" required by the strict baseline coaching prompt.*

## 6. Prompt Sensitivity & Signal Quality
> *Analyze the attempt to solve the sparsity issue via Prompt Engineering on the Pandas dataset.*

### 6.1 Precision vs. Recall (Medium vs. Radical)
> *Compare the `Pandas_Prompt_A_Individual` (Medium/Precision) and `Pandas_Prompt_B_Radical` (High Recall) runs. Show that relaxing the prompt successfully generated 192 GOs, proving the engine *can* find critiques.*
> `[INSERT SIGNAL INTENSITY SCATTER PLOT HERE]`

### 6.2 The "Nitpick" Problem
> *Note that while volume increased under the Radical prompt, the quality shifted. Many GOs became "surface-level" (e.g., "Clarify API docs" based on a single comment). Conclude that the Radical prompt confuses transactional code-corrections with behavioral coaching, establishing the "Medium/Individual" prompt as the most balanced approach.*

## 7. Engineering Success: The Kubernetes Run
> *Detail the results of the "Artificial Density" data-engineering solution combined with the balanced prompt.*

### 7.1 Unlocking Growth Opportunities Through Density
> *Show that by increasing network density (35%+) and utilizing the balanced "Individual Signal" prompt, the AI successfully triggered accurate GOs (e.g., the K8s_Artificial_Dense 4.6% yield). This proves the engine works when the data structure matches enterprise density and the prompt is properly tuned.*
> `[INSERT KUBERNETES NETWORK GRAPH HERE]`

## 8. Quantitative Insights & AI Stability
> *A visual, data-heavy section proving the AI is fair, unbiased, and accurate.*

### 8.1 The Overall Sentiment Landscape
> *Showcase the overall positive sentiment of the open-source community (mean score 6.63).*
> `[INSERT OVERALL SENTIMENT DISTRIBUTION GRAPH HERE]`

### 8.2 AI Fairness & Bias Checks
> *Use the Sentiment Boxplots and the "Score Delta" histogram to prove the AI's sentiment engine is highly robust and unbiased. It maintains stable scores (Mean Delta -0.05) regardless of how the coaching prompts are altered.*
> `[INSERT SENTIMENT BOXPLOT HERE]`
> `[INSERT GRUMPINESS DELTA HISTOGRAM HERE]`
> *Use the Pareto chart to prove critiques were distributed fairly, and the Length Scatter plot to prove the AI does not have a "verbosity bias."*
> `[INSERT PARETO GRAPH HERE]`
> `[INSERT LENGTH SCATTER PLOT HERE]`

## 9. Discussion: Context Saturation
> *Explore the final technical bottleneck discovered during the high-density Kubernetes runs.*

### 9.1 The "Lost in the Middle" Phenomenon
> *Briefly mention physical token limits, but focus on the theoretical issue: explaining how feeding an LLM thousands of dense technical comments at once dilutes behavioral signals, making it harder for the AI to connect the dots on soft skills.*

## 10. Conclusion & Future Work
> *Summarize the engine's proven capabilities in identifying soft skills and tone.*

> *Propose the architectural solution to Context Saturation: processing data in chronological "chunks" (e.g., daily/weekly ingestions) to simulate a real-world timeline, allowing the AI to build recurring patterns naturally without overwhelming the context window.*