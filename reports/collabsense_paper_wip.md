# CollabSense: Evaluating Enterprise AI Coaching Models on Open Source Collaboration Data

**Status:** Work in Progress  
**Project:** ClarityLoop / CollabSense  
**Focus:** AI Sentiment Analysis, Behavioral Coaching, and Graph Density in Open Source

---

## 1. Abstract
> *Brief overview of the project. Highlight the successful deployment of the ClarityLoop engine on public open-source data. Emphasise the model's high accuracy in mapping sentiment and identifying positive "Strengths." Summarise how combining an "Individual Signal" (medium) prompt with engineered "Artificial Density" successfully allowed the engine to generate accurate "Growth Opportunities."*

## 2. Introduction
## 2. Introduction & Objective

The ClarityLoop AI engine is designed to function as an automated behavioral coach within enterprise environments. By analysing daily workplace communications - such as peer-to-peer feedback, code reviews, and direct messaging - the engine identifies a team "Sentiment Score", highlights individual "Strengths", and surfaces actionable "Growth Opportunities" (constructive coaching moments) for employees. 

Historically, tools of this nature are optimised for the dense, hierarchical communication topologies typical of traditional corporate teams. The primary objective of this research is to evaluate the transferability of the ClarityLoop engine to the asynchronous, decentralised, and highly technical environment of Open Source Software (OSS) development. 

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

Because the AI is designed to analyse internal corporate communications, feeding it raw open-source data directly would not yield accurate results. The pipeline was broken into three main stages: asynchronous data mining using the GitHub API, logic-based filtering to isolate valuable interactions, and data anonymisation to simulate a corporate environment.

### 4.1 Dataset Selection

Two major open-source repositories were selected for this study: `pandas-dev/pandas` and `kubernetes/kubernetes`. These projects were chosen because of their massive scale, strict professional review standards, and highly active maintainer communities. These characteristics make them the closest available public proxy to a large, professional software engineering department.

**Filtering for Quality**
To ensure the AI was evaluating meaningful peer-to-peer feedback, strict filters were applied during the data ingestion phase:
* **Bot Removal:** Open-source repositories rely heavily on automation. The pipeline stripped out all interactions from known bots (e.g., accounts containing `[bot]`, `-bot`, or `bot-`) to guarantee the AI only analysed human behavior.
* **Long-Term Contributor Focus:** To replicate the dynamics of permanent employees, we implemented tenure and activity filters. For the baseline Pandas dataset, we filtered for users who had been consistently active over extended periods (ranging from 6 months up to 5 consecutive years, depending on the test) and who met minimum thresholds for participating in pull requests and issues. This effectively removed one-time "drive-by" contributors, leaving only the core community.

**Anonymisation and Workspace Simulation**
The ClarityLoop engine expects data formatted for a private enterprise organisation. To adapt the public GitHub data, an anonymisation module was built using the Python `Faker` library. 

This module systematically replaced all real GitHub usernames and public identities with synthetic profiles. It generated realistic fake names, corporate email addresses, user roles, and profile avatars. By mapping the filtered GitHub pull requests and comments to these synthetic profiles, we successfully generated clean, relational datasets (`users.csv`, `contexts.csv`, and `context_comments.csv`) that perfectly mirrored the structure of a private enterprise workspace. This allowed the engine to process the open-source data exactly as it would for a corporate client.

### 4.2 Summary of Tests

To isolate the variables of data structure and AI sensitivity (prompt engineering), the filtered data was processed through the ClarityLoop engine across several distinct experimental configurations. These configurations are referred to throughout the results:

*   **Pandas_ShortTerm & Pandas_LongTerm:** The baseline tests using the engine's default, strict enterprise coaching prompt. *LongTerm* represents the core dataset of 5-year consistent contributors.
*   **Pandas_Prompt_A_Individual (Medium/Precision):** A tuned prompt that bypassed the strict "recurring pattern" rule, allowing a single, high-quality piece of constructive feedback to trigger a Growth Opportunity.
*   **Pandas_Prompt_B_Radical (High Recall):** A highly relaxed "stress test" prompt that broadened the definition of a Growth Opportunity to capture almost any actionable critique.
*   **K8s_Artificial_Dense:** The Kubernetes dataset engineered using K-Core to simulate an enterprise team, processed using the balanced "Medium/Precision" prompt.
*   **K8s_Strict_Large:** A higher-density Kubernetes subset that ultimately highlighted context window saturation limits (discussed in Section 9).

| Experiment / Test ID | Analysed Comments (Volume) | Unique Reviewers | Unique Recipients | Total Strengths | Strength Yield (%) | Total Growth Opps | GO Yield (%) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Pandas_Prompt_B_Radical | 1226 | 32 | 83 | 41 | 3.34 | 192 | 15.66 |
| Pandas_LongTerm | 1162 | 63 | 194 | 120 | 10.33 | 0 | 0.00 |
| Pandas_Prompt_A_Individual | 479 | 20 | 69 | 39 | 8.14 | 5 | 1.04 |
| K8s_Strict_Large | 328 | 79 | 36 | 52 | 15.85 | 4 | 1.22 |
| K8s_Artificial_Dense | 87 | 48 | 17 | 12 | 13.79 | 4 | 4.60 |
| Pandas_ShortTerm | 32 | 5 | 15 | 6 | 18.75 | 0 | 0.00 |

## 5. Baseline Success & The Sparsity Challenge

The first dataset for the ClarityLoop engine test using real data was processed from the `pandas-dev/pandas` repository. This baseline test aimed to show if the AI could analyse open-source technical discussions and extract meaningful insights using its default settings. The results demonstrated both a clear success in recognising positive collaboration, and a limitation for the "growth opportunity" growth signal.

### 5.1 Validating Soft-Skill Extraction

The engine successfully processed 1,162 comments from 63 unique users in the baseline test. From this data, it generated 120 distinct "Strengths," a 10.3% yield rate. This high volume of positive reinforcement confirms that the tuned AI is fully capable of understanding the context and intent of open-source code reviews.

More importantly, rather than just extracting technical programming keywords, the engine accurately identified high-level soft skills. As shown in **Figure 3 (Total Strengths Generated)**, this baseline run produced the highest volume of positive signals across all experiments. 

![total_strengths_volume](../dataset/initial%20visualizations%20created%20from%20data/total_strengths_volume.png)

Breaking down these signals further, **Table 2 (Top 3 Strengths)** shows that the engine successfully identified leadership and teamwork behaviors. The most frequent positive traits flagged were "Engages in collaborative problem-solving" (19 occurrences), "Proposes practical solutions" (7 occurrences), and "Proactively seeks feedback" (7 occurrences). This validates the model's core comprehension abilities in non-corporate environments.

| Rank | Strength Title | Count |
| :---: | :--- | :---: |
| **1** | Engages in collaborative problem-solving | 19 |
| **2** | Proposes practical solutions | 7 |
| **3** | Proactively seeks feedback | 7 |

### 5.2 The Missing Growth Opportunities

While the engine successfully extracted positive traits in the baseline `Pandas_LongTerm` run, it failed to generate any actionable "Growth Opportunities" (0 yield). Further analysis of the network topology of interactions between users revealed a flaw outside of a simple failure of comprehension.

The standard ClarityLoop engine is tuned for enterprise environments. Its logic demands that actionable coaching advice only be generated when "recurring behavioral patterns" are identified across multiple interactions from the same peer group. This prevents the AI from flagging one-off misunderstandings as an mistake a user should grow from.

Network analysis revealed the Pandas dataset had an interaction density of just 1.1% (the ratio of actual peer-to-peer interactions out of all possible user pairings). In this sparse, transactional environment, meeting the "recurring pattern" requirement is statistically impossible. The AI correctly recognized the lack of sustained coaching, resulting in a zero yield.

By changing the AI's prompt (Section 6) or changing the data structure itself (Section 7) became the primary focus of the subsequent tests.

## 6. Prompt Sensitivity & Signal Quality

Rather than immediately undertaking data engineering to fix the 1.1% network sparsity, the first approach was to test if the limitation could be bypassed entirely via Prompt Engineering. The objective was to determine if the ClarityLoop engine *could* generate growth opportunities from sparse open-source data if the strict "recurring patterns" requirement was removed.

Two new configurations were tested on the Pandas dataset: a "Medium/Precision" prompt (`Pandas_Prompt_A_Individual`), which allowed a single high-quality piece of constructive feedback to trigger an insight, and a "High Recall" prompt (`Pandas_Prompt_B_Radical`), which broadened the definition of "growth" to include almost any actionable improvement.

### 6.1 Precision vs. Recall (Medium vs. Radical)

Relaxing the prompt parameters immediately solved the zero-yield problem. As shown in **Figure 4**, both modified prompts successfully forced the AI to generate Growth Opportunities from the sparse open-source data. 

![signal_intensity_scatter](../dataset/initial%20visualizations%20created%20from%20data/signal_intensity_scatter.png)

The `Pandas_Prompt_A_Individual` (Medium/Precision) run processed 479 comments and generated 5 high-quality Growth Opportunities (a conservative 1% yield). This configuration acted much like a thoughtful peer review, ensuring the advice remained strategic. 

Conversely, the `Pandas_Prompt_B_Radical` (High Recall) run acted as a stress test for the engine's capabilities. It processed 1,226 comments and identified 192 Growth Opportunities, achieving a massive 15.6% yield rate. This proved definitively that the AI possesses the deep comprehension necessary to identify critiques in technical text, provided the prompt threshold is lowered sufficiently.

### 6.2 The "Nitpick" Problem

While the `Pandas_Prompt_B_Radical` configuration successfully generated a high volume of Growth Opportunities, a qualitative review showed a large in the nature of the feedback. The engine often confused transactional code-corrections with behavioral coaching.

Because the threshold for a growth opportunity was so low, the AI frequently flagged surface-level development comments, such as "Clarify API documentation for public exposure" or requests for shorter bug reports, as personal Growth Opportunities. While accurate to the text, these are standard open-source "nitpicks" and code-review tasks, not developmental soft-skill coaching. 

This establishes a clear trade-off: forcing the AI to find feedback in a sparse network via highly relaxed prompts introduces unacceptable noise. It proves that the engine requires a balanced, "Medium" prompt to filter for quality, meaning the underlying network density itself must be fixed to generate valid results.

## 7. Engineering Success: The Kubernetes Run

Because relaxing the AI’s prompt (Section 6) led to unacceptable noise and surface-level "nitpicks," it was clear that the engine's strict coaching standards needed to be maintained. Therefore, best path forward was to address the root cause of the problem: the 1.1% network sparsity. If open-source collaboration is naturally too sparse to trigger behavioral patterns, we needed to artificially engineer an enterprise-level interaction density.

**K-Core Decomposition & The Ingestion Error**
To achieve this, the focus shifted to the massive Kubernetes repository. We applied K-Core decomposition, a graph theory algorithm that identifies the "densest subgraph" of frequent collaborators. However, simply downloading the entire comment history for these core maintainers inadvertently pulled in thousands of one-off interactions with external public contributors. This diluted the dataset to a 0.2% density, creating noise around the core team.

![Network Density Graph](./data/kubernetes-density-focused/network_density_graph.png)

**The Strict Peer-to-Peer Filter**
To solve this dilution, a strict peer-to-peer allow-list was implemented. The data pipeline was modified to delete any comment where either the sender or the recipient was outside the identified core group. 

This stripped away the transactional noise, revealing a highly interconnected subset of 22 to 55 core maintainers. By artificially engineering these datasets to achieve internal densities ranging from 35% to 75%, the pipeline successfully replicated the dense communication patterns of a close-knit corporate engineering department.

![Core Team Graph](./data/kubernetes-density-focused/core_team_graph.png)

### 7.1 Unlocking Growth Opportunities Through Density

With a high-density dataset successfully engineered, the data was processed through the ClarityLoop engine. Crucially, this was run using the balanced "Medium/Precision" prompt. This ensured the AI maintained its strict developmental standards and did not revert to the "nitpick" behavior seen in Section 6.

The results definitively validated the structural hypothesis. In the `K8s_Artificial_Dense` run, the engine processed 87 high-density peer-to-peer comments and generated 4 highly accurate Growth Opportunities, achieving a healthy 4.6% yield. 

![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/k8_network_graph.png)

Rather than a single uniform web, the graph forms distinct "hub-and-spoke" clusters. A few central maintainers act as hubs, repeatedly interacting with specific subgroups of contributors. This locally dense structure mimics the "Manager and Direct Reports" dynamic of a traditional enterprise department. Because the AI requires recurring feedback between the same individuals to trigger a coaching insight, these isolated, high-interaction clusters provided the exact environment needed to generate the Growth Opportunities.

The `K8s_Strict_Large` run saw similar success, generating 4 Growth Opportunities from 328 comments (1.2% yield). 

Unlike the transactional feedback generated by the Radical prompt, the Growth Opportunities generated here were strategic and developmental (e.g., identifying patterns of premature PR approvals). This outcome proves that the ClarityLoop engine is fully capable of extracting complex, behavioral coaching signals from open-source technical data. However, it requires both a properly tuned prompt *and* an interaction graph that artificially mirrors the dense, recurring relationships of a corporate department.

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
> *Summarise the engine's proven capabilities in identifying soft skills and tone.*

> *Propose the architectural solution to Context Saturation: processing data in chronological "chunks" (e.g., daily/weekly ingestions) to simulate a real-world timeline, allowing the AI to build recurring patterns naturally without overwhelming the context window.*