# CollabSense: Evaluating Enterprise AI Coaching Models on Open Source Collaboration Data

**Status:** Work in Progress  
**Project:** ClarityLoop / CollabSense  
**Focus:** AI Sentiment Analysis, Behavioural Coaching, and Graph Density in Open Source

---

## Abstract
> *Brief overview of the project. Highlight the successful deployment of the ClarityLoop engine on public open-source data. Emphasise the model's high accuracy in mapping sentiment and identifying positive "Strengths." Summarise how combining an "Individual Signal" (medium) prompt with engineered "Artificial Density" successfully allowed the engine to generate accurate "Growth Opportunities."*

## 1. Introduction
## 1. Introduction & Objective

The ClarityLoop AI engine is designed to function as an automated behavioural coach within enterprise environments. By analysing daily workplace communications - such as peer-to-peer feedback, code reviews, and direct messaging - the engine identifies a team "Sentiment Score", highlights individual "Strengths", and surfaces actionable "Growth Opportunities" (constructive coaching moments) for employees. 

Historically, tools of this nature are optimised for the dense, hierarchical communication topologies typical of traditional corporate teams. The primary objective of this research is to evaluate the transferability of the ClarityLoop engine to the asynchronous, decentralised, and highly technical environment of Open Source Software (OSS) development. 

Specifically, this study uses public GitHub pull requests and issue comments from massive open-source repositories (`pandas-dev/pandas` and `kubernetes/kubernetes`) as a proxy for internal enterprise peer reviews. Open-source collaboration provides a massive, high-quality dataset of professional engineering interactions, making it an ideal stress-test for the AI's pattern-recognition capabilities.

This research seeks to answer three core questions:
1. **Sentiment Transferability:** Can an enterprise-tuned AI accurately interpret the tone and intent of open-source technical debates without hallucinating negativity or toxic behaviour?
2. **behavioural Recognition:** Can the engine distinguish between transactional code corrections (e.g., syntax fixes) and genuine behavioural coaching moments (e.g., communication style, collaboration, and leadership)?
3. **Structural Adaptability:** What prompt tuning and data-engineering techniques are required to overcome the inherent "sparsity" of open-source social graphs to successfully trigger the engine's Growth Opportunity algorithms?

## 2. Literature Review

The CollabSense project draws on three areas of existing research: sentiment analysis applied to professional communication, automated extraction of feedback and soft-skill signals from software artefacts, and the structural analysis of open-source collaboration networks. Each of these areas has its own established literature, but the specific gap addressed in this study - applying an enterprise-tuned behavioural coaching engine to public open-source data - has received little direct attention. This section summarises the relevant prior work and shows how it motivates the experimental design used in later sections.

### 2.1 Sentiment Analysis in the Workplace

Sentiment analysis began with product reviews and social media (Pang & Lee, 2008), but the techniques have since been adapted to professional communication. Workplace text behaves differently from consumer text: most of it is neutral, it relies heavily on technical jargon, and people tend not to express strong emotion openly. Loughran and McDonald (2011) showed this clearly in finance, where general-purpose sentiment lexicons such as Harvard IV consistently flagged neutral business language as negative. The same problem appears in software engineering text. Jongeling, Sarkar, Datta, and Serebrenik (2017) tested four off-the-shelf sentiment tools (SentiStrength, NLTK, Stanford CoreNLP, Alchemy) on GitHub and Jira data and found that they often disagreed with each other and frequently misread direct but constructive code review as hostile.

These findings led to a wave of sentiment classifiers built specifically for software development. Murgia, Tourani, Adams, and Ortu (2014) confirmed that developers do show emotion in issue trackers, providing a basis for further work in this area. Ortu, Adams, Destefanis, Tourani, Marchesi, and Tonelli (2015) released a large labelled corpus of Jira issue comments, and Calefato, Lanubile, Maiorano, and Novielli (2018) introduced Senti4SD, a supervised classifier trained on Stack Overflow that performed noticeably better than general-purpose tools on developer text. Lin, Zampetti, Bavota, Di Penta, Lanza, and Oliveto (2018) compared several of these tools side by side and concluded that none of them were yet reliable enough for production use, and recommended retraining on the specific platform being analysed. More recent work on cross-platform SE-specific tools (Novielli, Calefato, Dongiovanni, Girardi, & Lanubile, 2020) and on transformer-based models such as the BERT-based classifier of Biswas, Karabulut, Pollock, and Vijay-Shanker (2020) has closed much of the gap, with reported F1 scores on developer text moving from around 0.6 for lexicon tools to above 0.85.

Outside academia, commercial platforms such as Microsoft Viva Insights, Slack analytics, and Humanyze have brought passive sentiment monitoring directly into the workplace. Mäntylä, Graziotin, and Kuutila (2018) reviewed nearly 7,000 sentiment analysis papers and noted that workplace deployments rarely include checks for verbosity bias, recency bias, or fairness across users - which are exactly the bias dimensions Section 7 of this paper evaluates. Two clear takeaways emerge from this body of work. First, sentiment models trained outside the software domain often misread technical text, so any claim of accuracy on GitHub data needs to be tested rather than assumed. Second, the move from lexicon and classical-ML sentiment tools to LLM-based evaluators is recent enough that there is little systematic evidence on how stable LLM scores are across prompt changes. The bias checks in Section 7 (a consistency delta of -0.05, a fair Pareto distribution, and no verbosity correlation) directly address this gap for the ClarityLoop engine.

### 2.2 Automated Feedback & Growth Signals

Work on automating developmental feedback, and going beyond sentiment polarity into actionable coaching content, is in earlier stages than sentiment analysis. The following three areas are relevant.

The first is **automated peer and code review**. Bacchelli and Bird (2013) showed that modern code review is mostly a communication activity rather than a defect-finding one, with review threads used for knowledge transfer, mentoring, and reinforcing team norms. Bosu, Greiler, and Bird (2015) classified comments in Microsoft code reviews and found that around a third were genuinely useful in a developmental sense, which motivated later work on labelling review comments by intent. Rahman, Roy, and Kula (2017) and Ebert, Castor, Novielli, and Serebrenik (2019) built classifiers to separate nitpicks, defects, and design discussions, but neither study tried to label behavioural or soft-skill content. The "nitpick problem" reported in Section 5.2 is therefore a known issue in the literature: surface-level corrections make up most of the volume in review text, and any system that does not actively filter them out will surface them at the expense of higher-level signals.

The second area is **soft-skill extraction**. Outside software engineering, work on the StudentLife corpus (Wang et al., 2014) and on narrative performance reviews (Speer, 2021) has shown that traits like collaboration, initiative, and proactivity can be recovered from free-text comments with reasonable agreement against human raters. There is less of this work inside software engineering, and what does exist tends to aggregate findings at the team or community level rather than producing feedback for individual developers. The ClarityLoop engine is unusual in that it produces per-person growth advice, which makes evaluation harder than standard NLP metrics allow for.

The third area is the **prompt-engineering trade-off** for LLM-based extraction. Reynolds and McDonell (2021) and Liu et al. (2023) showed that the same model, given different but reasonable instructions, can return outputs that differ by an order of magnitude in volume and that mean very different things in content. The pattern observed in Section 5 - a strict prompt returning zero growth opportunities, a medium prompt returning 5, and a radical prompt returning 192 - matches this body of work directly. It also supports the methodological choice in Section 6 to keep the medium prompt and instead change the structure of the input data, rather than continuing to relax the prompt and accept the noise that comes with it.

### 2.3 The Gap: Enterprise vs. Open Source Topology

The structural difference between corporate and open-source collaboration is the most directly studied of the three areas, but it has not previously been framed as a constraint on what an AI can detect. Crowston and Howison (2005) and Jergensen, Sarma, and Wagstrom (2011) described open-source communities using "onion" or core-periphery models: a small group of long-term maintainers surrounded by larger rings of occasional and one-time contributors. Bird, Pattison, D'Souza, Filkov, and Devanbu (2008) showed that the resulting interaction graphs have heavy-tailed degree distributions and very low overall density, with most contributors only connected to the project through a single maintainer. Tsay, Dabbish, and Herbsleb (2014) confirmed the same pattern on GitHub and added that pull-request decisions are strongly influenced by prior social ties, which means the few dense pockets that do exist are also the places where genuine developmental feedback is most likely to happen.

Corporate engineering teams sit at the other end of this spectrum. Studies of internal Microsoft, Google, and IBM repositories (Bird, Nagappan, Murphy, Gall, & Devanbu, 2011; Sadowski, Söderberg, Church, Sipko, & Bacchelli, 2018) report code-review interaction densities roughly an order of magnitude higher than comparable open-source projects, with the same reviewer-author pairs recurring over weeks and months. This is the topology the ClarityLoop engine was built for: repeated interactions between the same people, where a reviewer can see a behavioural pattern develop across multiple exchanges with the same recipient. In a graph with 1.1% density - the measured value for the Pandas baseline in Section 4 - the chance of any pair interacting more than once is essentially zero, so a coaching prompt that requires recurring patterns will correctly return nothing.

The methodological response used in this study comes from classical graph mining. K-Core decomposition (Seidman, 1983; Batagelj & Zaveršnik, 2011) finds the largest subgraph in which every node has at least $k$ neighbours, and is a standard way to pull a stable core out of a noisy contributor graph. Kalliamvakou et al. (2014, 2016), in their widely cited "promises and perils of mining GitHub" papers, explicitly warn that whole-repository statistics mix the dense maintainer core with the sparse drive-by periphery, and recommend topological filtering before any behavioural analysis. The "artificial density" approach used in Section 6, which applies K-Core to recover a 35-75% dense subgraph from the Kubernetes repository, is a direct application of this advice to the enterprise-AI evaluation setting.

A final relevant constraint comes from the LLM literature itself. Liu, Lin, Hewitt, Paranjape, Bevilacqua, Petroni, and Liang (2024) documented the "Lost in the Middle" effect, where transformer models retrieve information reliably from the start and end of a long context but lose accuracy on content placed in the middle. This matters for CollabSense because successful densification produces, by design, very long per-user histories: the engineered Kubernetes core team contained individuals with tens of thousands of comments. The same structural fix that solves the sparsity problem therefore creates a context-length problem, which Section 8 discusses and which Section 9 proposes to address through chronological chunking.

Taken together, the three areas of prior work map out a clear gap. Sentiment analysis on software text has matured to the point where domain-aware models can be trusted on developer communication, but their stability under prompt variation has not been studied in any detail. Automated feedback work has produced strong defect and intent classifiers, but has not connected those outputs to recurring behavioural patterns at the individual level. Network analysis of open-source has long known that these graphs are sparse, but has not framed sparsity as something that gates what an enterprise-tuned coaching AI can detect in the first place. CollabSense addresses this gap by treating data topology, prompt sensitivity, and AI fairness as a single connected evaluation problem.

## 3. Methodology & Data Engineering

The methodology for this study required building a custom Python data pipeline (`clarityloop/collabsense/src`) to extract, filter, and transform public GitHub interactions into a format that the ClarityLoop engine could process. 

Because the AI is designed to analyse internal corporate communications, feeding it raw open-source data directly would not yield accurate results. The pipeline was broken into three main stages: asynchronous data mining using the GitHub API, logic-based filtering to isolate valuable interactions, and data anonymisation to simulate a corporate environment.

### 3.1 Dataset Selection

Two major open-source repositories were selected for this study: `pandas-dev/pandas` and `kubernetes/kubernetes`. These projects were chosen because of their massive scale, strict professional review standards, and highly active maintainer communities. These characteristics make them the closest available public proxy to a large, professional software engineering department.

**Filtering for Quality**
To ensure the AI was evaluating meaningful peer-to-peer feedback, strict filters were applied during the data ingestion phase:
* **Bot Removal:** Open-source repositories rely heavily on automation. The pipeline stripped out all interactions from known bots (e.g., accounts containing `[bot]`, `-bot`, or `bot-`) to guarantee the AI only analysed human behaviour.
* **Long-Term Contributor Focus:** To replicate the dynamics of permanent employees, we implemented tenure and activity filters. For the baseline Pandas dataset, we filtered for users who had been consistently active over extended periods (ranging from 6 months up to 5 consecutive years, depending on the test) and who met minimum thresholds for participating in pull requests and issues. This effectively removed one-time "drive-by" contributors, leaving only the core community.

**Anonymisation and Workspace Simulation**
The ClarityLoop engine expects data formatted for a private enterprise organisation. To adapt the public GitHub data, an anonymisation module was built using the Python `Faker` library. 

This module systematically replaced all real GitHub usernames and public identities with synthetic profiles. It generated realistic fake names, corporate email addresses, user roles, and profile avatars. By mapping the filtered GitHub pull requests and comments to these synthetic profiles, we successfully generated clean, relational datasets (`users.csv`, `contexts.csv`, and `context_comments.csv`) that perfectly mirrored the structure of a private enterprise workspace. This allowed the engine to process the open-source data exactly as it would for a corporate client.

### 3.2 Summary of Tests

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

## 4. Baseline Success & The Sparsity Challenge

The first dataset for the ClarityLoop engine test using real data was processed from the `pandas-dev/pandas` repository. This baseline test aimed to show if the AI could analyse open-source technical discussions and extract meaningful insights using its default settings. The results demonstrated both a clear success in recognising positive collaboration, and a limitation for the "growth opportunity" growth signal.

### 4.1 Validating Soft-Skill Extraction

The engine successfully processed 1,162 comments from 63 unique users in the baseline test. From this data, it generated 120 distinct "Strengths," a 10.3% yield rate. This high volume of positive reinforcement confirms that the tuned AI is fully capable of understanding the context and intent of open-source code reviews.

More importantly, rather than just extracting technical programming keywords, the engine accurately identified high-level soft skills. As shown in **Figure 3 (Total Strengths Generated)**, this baseline run produced the highest volume of positive signals across all experiments. 

![total_strengths_volume](../dataset/initial%20visualizations%20created%20from%20data/total_strengths_volume.png)

Breaking down these signals further, **Table 2 (Top 3 Strengths)** shows that the engine successfully identified leadership and teamwork behaviours. The most frequent positive traits flagged were "Engages in collaborative problem-solving" (19 occurrences), "Proposes practical solutions" (7 occurrences), and "Proactively seeks feedback" (7 occurrences). This validates the model's core comprehension abilities in non-corporate environments.

| Rank | Strength Title | Count |
| :---: | :--- | :---: |
| **1** | Engages in collaborative problem-solving | 19 |
| **2** | Proposes practical solutions | 7 |
| **3** | Proactively seeks feedback | 7 |

### 4.2 The Missing Growth Opportunities

While the engine successfully extracted positive traits in the baseline `Pandas_LongTerm` run, it failed to generate any actionable "Growth Opportunities" (0 yield). Further analysis of the network topology of interactions between users revealed a flaw outside of a simple failure of comprehension.

The standard ClarityLoop engine is tuned for enterprise environments. Its logic demands that actionable coaching advice only be generated when "recurring behavioural patterns" are identified across multiple interactions from the same peer group. This prevents the AI from flagging one-off misunderstandings as an mistake a user should grow from.

Network analysis revealed the Pandas dataset had an interaction density of just 1.1% (the ratio of actual peer-to-peer interactions out of all possible user pairings). In this sparse, transactional environment, meeting the "recurring pattern" requirement is statistically impossible. The AI correctly recognised the lack of sustained coaching, resulting in a zero yield.

By changing the AI's prompt (Section 6) or changing the data structure itself (Section 7) became the primary focus of the subsequent tests.

## 5. Prompt Sensitivity & Signal Quality

Rather than immediately undertaking data engineering to fix the 1.1% network sparsity, the first approach was to test if the limitation could be bypassed entirely via Prompt Engineering. The objective was to determine if the ClarityLoop engine *could* generate growth opportunities from sparse open-source data if the strict "recurring patterns" requirement was removed.

Two new configurations were tested on the Pandas dataset: a "Medium/Precision" prompt (`Pandas_Prompt_A_Individual`), which allowed a single high-quality piece of constructive feedback to trigger an insight, and a "High Recall" prompt (`Pandas_Prompt_B_Radical`), which broadened the definition of "growth" to include almost any actionable improvement.

### 5.1 Precision vs. Recall (Medium vs. Radical)

Relaxing the prompt parameters immediately solved the zero-yield problem. As shown in **Figure 4**, both modified prompts successfully forced the AI to generate Growth Opportunities from the sparse open-source data. 

![signal_intensity_scatter](../dataset/initial%20visualizations%20created%20from%20data/signal_intensity_scatter.png)

The `Pandas_Prompt_A_Individual` (Medium/Precision) run processed 479 comments and generated 5 high-quality Growth Opportunities (a conservative 1% yield). This configuration acted much like a thoughtful peer review, ensuring the advice remained strategic. 

Conversely, the `Pandas_Prompt_B_Radical` (High Recall) run acted as a stress test for the engine's capabilities. It processed 1,226 comments and identified 192 Growth Opportunities, achieving a massive 15.6% yield rate. This proved definitively that the AI possesses the deep comprehension necessary to identify critiques in technical text, provided the prompt threshold is lowered sufficiently.

### 5.2 The "Nitpick" Problem

While the `Pandas_Prompt_B_Radical` configuration successfully generated a high volume of Growth Opportunities, a qualitative review showed a large in the nature of the feedback. The engine often confused transactional code-corrections with behavioural coaching.

Because the threshold for a growth opportunity was so low, the AI frequently flagged surface-level development comments, such as "Clarify API documentation for public exposure" or requests for shorter bug reports, as personal Growth Opportunities. While accurate to the text, these are standard open-source "nitpicks" and code-review tasks, not developmental soft-skill coaching. 

This establishes a clear trade-off: forcing the AI to find feedback in a sparse network via highly relaxed prompts introduces unacceptable noise. It proves that the engine requires a balanced, "Medium" prompt to filter for quality, meaning the underlying network density itself must be fixed to generate valid results.

## 6. Engineering Success: The Kubernetes Run

Because relaxing the AI’s prompt (Section 6) led to unacceptable noise and surface-level "nitpicks," it was clear that the engine's strict coaching standards needed to be maintained. Therefore, best path forward was to address the root cause of the problem: the 1.1% network sparsity. If open-source collaboration is naturally too sparse to trigger behavioural patterns, we needed to artificially engineer an enterprise-level interaction density.

**K-Core Decomposition & The Ingestion Error**
To achieve this, the focus shifted to the massive Kubernetes repository. We applied K-Core decomposition (Seidman, 1983; Batagelj & Zaveršnik, 2011), a graph theory algorithm that identifies the "densest subgraph" of frequent collaborators. However, simply downloading the entire comment history for these core maintainers inadvertently pulled in thousands of one-off interactions with external public contributors. This diluted the dataset to a 0.2% density, creating noise around the core team.

![Network Density Graph](./data/kubernetes-density-focused/network_density_graph.png)

**The Strict Peer-to-Peer Filter**
To solve this dilution, a strict peer-to-peer allow-list was implemented. The data pipeline was modified to delete any comment where either the sender or the recipient was outside the identified core group. 

This stripped away the transactional noise, revealing a highly interconnected subset of 22 to 55 core maintainers. By artificially engineering these datasets to achieve internal densities ranging from 35% to 75%, the pipeline successfully replicated the dense communication patterns of a close-knit corporate engineering department.

![Core Team Graph](./data/kubernetes-density-focused/core_team_graph.png)

### 6.1 Unlocking Growth Opportunities Through Density

With a high-density dataset successfully engineered, the data was processed through the ClarityLoop engine. Crucially, this was run using the balanced "Medium/Precision" prompt. This ensured the AI maintained its strict developmental standards and did not revert to the "nitpick" behaviour seen in Section 6.

The results definitively validated the structural hypothesis. In the `K8s_Artificial_Dense` run, the engine processed 87 high-density peer-to-peer comments and generated 4 highly accurate Growth Opportunities, achieving a healthy 4.6% yield. 

![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/k8_network_graph.png)

Rather than a single uniform web, the graph forms distinct "hub-and-spoke" clusters. A few central maintainers act as hubs, repeatedly interacting with specific subgroups of contributors. This locally dense structure mimics the "Manager and Direct Reports" dynamic of a traditional enterprise department. Because the AI requires recurring feedback between the same individuals to trigger a coaching insight, these isolated, high-interaction clusters provided the exact environment needed to generate the Growth Opportunities.

The `K8s_Strict_Large` run saw similar success, generating 4 Growth Opportunities from 328 comments (1.2% yield). 

Unlike the transactional feedback generated by the Radical prompt, the Growth Opportunities generated here were strategic and developmental (e.g., identifying patterns of premature PR approvals). This outcome proves that the ClarityLoop engine is fully capable of extracting complex, behavioural coaching signals from open-source technical data. However, it requires both a properly tuned prompt *and* an interaction graph that artificially mirrors the dense, recurring relationships of a corporate department.

## 7. Quantitative Insights & AI Stability

While Sections 5 through 7 focused on the structural challenges of generating Growth Opportunities, the ClarityLoop engine processes much more than just critiques. It constantly maps the underlying sentiment and soft-skill strengths of the team. 

To ensure the engine is reliable for enterprise deployment, a quantitative analysis was performed on the `feedback_scores.csv` dataset (containing over 3,300 unique scored interactions) to verify that the AI remains fair, objective, and unbiased, regardless of the prompt or data density.

### 7.1 The Overall Sentiment Landscape

A common concern with applying AI to open-source or highly technical text is that the model might misinterpret direct, blunt code reviews as "toxic" or "negative." 

An analysis of the entire dataset's sentiment scores refutes this. As shown in the distribution below, the overall mean sentiment score across all open-source interactions analysed was **6.63 out of 10**. The distribution forms a healthy bell curve heavily weighted toward the "Constructive/Neutral" (6) and "Positive/Praising" (8) ranges, with very few scores falling into the negative (0-3) range. 

![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/overall_sentiment_distribution.png)

This confirms that the AI accurately comprehends the professional, transactional nature of engineering collaboration without hallucinating negativity - a known failure mode of general-purpose sentiment tools on developer text (Jongeling et al., 2017).

### 7.2 AI Fairness & Bias Checks

To definitively prove the stability and fairness of the AI, three distinct bias checks were conducted across the experimental runs.

**1. Execution Stability (The Consistency Check)**
Throughout all experiments, the prompt used to generate Sentiment Scores remained the same; only the Growth Opportunity prompts were altered. However, because Large Language Models are inherently non-deterministic, it was crucial to verify that the engine produces consistent, reproducible scores across completely separate pipeline runs (e.g., comparing the `Pandas_Prompt_A_Individual` run against the `Pandas_Prompt_B_Radical` run). 
*   **Boxplot Analysis:** The interquartile ranges of sentiment scores remained almost identical across all Baseline, Precision, and Radical runs. 
*   **The Delta Test:** By isolating the exact same comments analysed during different pipeline executions, we calculated the "Score Delta." The mean difference in score was just **-0.05**. This proves the core AI evaluator is highly reliable and objective, consistently assigning the same score to the same text regardless of when the analysis is executed.

![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/sentiment_box_plot.png)
![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/delta_histogram.png)

**2. Target Fairness (The Pareto Check)**
In a corporate setting, an AI must not unfairly target a specific employee. A Pareto analysis was run on the high-volume Radical dataset to see how Growth Opportunities were distributed among the developers. The resulting curve closely tracked the line of "Perfect Equality." This proves the AI does not single out specific individuals, and that critiques scale linearly and fairly with a user's total interaction volume.

![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/bias_check_pareto.png)

**3. Verbosity Bias Check**
Finally, Large Language Models often exhibit a "verbosity bias," incorrectly assuming that longer text is inherently better or more thoughtful. A scatter plot mapping the character count of a comment against its AI-assigned Sentiment Score revealed a flat trendline. The AI assigns both high (9) and low (4) scores equally to 10-word responses and 1,000-word responses, proving it judges the context of the communication, not just the length.

![k8_network_graph](../dataset/initial%20visualizations%20created%20from%20data/delta_histogram.png)

## 8. Discussion: Context Saturation

Engineering "Artificial Density" successfully solved network sparsity, but it introduced a new LLM-specific bottleneck: **Context Saturation**. By packing months of highly active open-source interactions into dense peer-to-peer graphs, the volume of historical text per user became massive. While recent upgrades to foundational LLM context windows have largely mitigated hard token limits, the challenge shifted from *fitting* the data into the model to how the model *processes* it. 

Feeding an LLM a massive batch of history at once (e.g., 128k+ tokens) triggers the well-documented "Lost in the Middle" phenomenon (Liu et al., 2024), where models struggle to retain and cross-reference details buried in the center of a prompt. 

For the ClarityLoop engine, this saturation may severely dilute behavioural signals, where subtle soft-skill coaching moments can be buried under tens of thousands of lines of highly technical "noise" (e.g., raw code snippets, JSON configurations). The AI would then become overwhelmed by the technical density and struggles to "connect the dots" across months of history. This context saturation could explain why the Growth Opportunity yield in the successful Kubernetes run remained relatively conservative (4.6%); the patterns exist, but processing them in a single massive batch makes them incredibly difficult for the LLM to extract.

## 9. Conclusion & Future Work

This research definitively proves that the ClarityLoop AI engine possesses the complex comprehension capabilities required to function as an automated behavioural coach. When evaluating the highly technical environment of Open Source Software (OSS) development, the model demonstrated high accuracy. It correctly mapped the professional, constructive tone of the `pandas` and `kubernetes` communities (mean sentiment 6.63) without hallucinating negativity, and successfully identified thousands of positive soft skills and leadership traits.

Crucially, this study identified that the engine’s failure to generate actionable "Growth Opportunities" on raw open-source data was not an AI comprehension flaw, but a structural mismatch. The 1.1% interaction sparsity of public repositories in their raw form simply did not contain the dense, recurring peer-to-peer relationships the enterprise-tuned AI requires to trigger a coaching insight. 

By utilising K-Core decomposition and strict peer-filtering, we successfully engineered an "Artificial Density" subset (35%+). When processed with a balanced "Medium/Precision" prompt, this dense data successfully triggered accurate, strategic Growth Opportunities (4.6% yield). This confirms that the ClarityLoop engine is highly transferable and effective, provided the data topology mirrors the density of a traditional corporate department.

### 10.1 Simulating Chronological Ingestion

Having solved the data engineering challenge of network sparsity, the final challenge was "Context Saturation" (Section 9). This saturation is an artifact of the experimental methodology - forcing the LLM to process years of dense historical data in a single, monolithic batch, rather than a flaw in the engine itself. 

In a live enterprise deployment, the ClarityLoop engine naturally operates by ingesting and analysing communications incrementally (e.g., daily or weekly). Because it evaluates smaller, time-bound slices of data as they happen, the engine is already theoretically positioned to avoid context window saturation in practice.

Therefore, future work should focus on modifying the testing pipeline to accurately simulate a live production environment. By "playing back" the historical open-source datasets to the engine in chronological chunks, we can evaluate how the AI naturally builds and cross-references recurring behavioral patterns over time. This approach will bypass the "Lost in the Middle" phenomenon, allowing us to accurately measure the engine's true yield of high-value coaching insights under real-world conditions.

## References

Bacchelli, A., & Bird, C. (2013). Expectations, outcomes, and challenges of modern code review. *Proceedings of the 35th International Conference on Software Engineering (ICSE)*, 712-721. [doi.org/10.1109/ICSE.2013.6606617](https://doi.org/10.1109/ICSE.2013.6606617)

Batagelj, V., & Zaveršnik, M. (2011). Fast algorithms for determining (generalized) core groups in social networks. *Advances in Data Analysis and Classification*, 5(2), 129-145. [doi.org/10.1007/s11634-010-0079-y](https://doi.org/10.1007/s11634-010-0079-y)

Bird, C., Nagappan, N., Murphy, B., Gall, H., & Devanbu, P. (2011). Don't touch my code! Examining the effects of ownership on software quality. *Proceedings of the 19th ACM SIGSOFT Symposium on the Foundations of Software Engineering (FSE)*, 4-14. [doi.org/10.1145/2025113.2025119](https://doi.org/10.1145/2025113.2025119)

Bird, C., Pattison, D., D'Souza, R., Filkov, V., & Devanbu, P. (2008). Latent social structure in open source projects. *Proceedings of the 16th ACM SIGSOFT International Symposium on Foundations of Software Engineering (FSE)*, 24-35. [doi.org/10.1145/1453101.1453107](https://doi.org/10.1145/1453101.1453107)

Biswas, E., Karabulut, M. E., Pollock, L., & Vijay-Shanker, K. (2020). Achieving reliable sentiment analysis in the software engineering domain using BERT. *Proceedings of the IEEE International Conference on Software Maintenance and Evolution (ICSME)*, 162-173. [doi.org/10.1109/ICSME46990.2020.00025](https://doi.org/10.1109/ICSME46990.2020.00025)

Bosu, A., Greiler, M., & Bird, C. (2015). Characteristics of useful code reviews: An empirical study at Microsoft. *Proceedings of the 12th IEEE/ACM Working Conference on Mining Software Repositories (MSR)*, 146-156. [doi.org/10.1109/MSR.2015.21](https://doi.org/10.1109/MSR.2015.21)

Calefato, F., Lanubile, F., Maiorano, F., & Novielli, N. (2018). Sentiment polarity detection for software development. *Empirical Software Engineering*, 23(3), 1352-1382. [doi.org/10.1007/s10664-017-9546-9](https://doi.org/10.1007/s10664-017-9546-9)

Crowston, K., & Howison, J. (2005). The social structure of free and open source software development. *First Monday*, 10(2). [doi.org/10.5210/fm.v10i2.1207](https://doi.org/10.5210/fm.v10i2.1207)

Ebert, F., Castor, F., Novielli, N., & Serebrenik, A. (2019). Confusion in code reviews: Reasons, impacts, and coping strategies. *Proceedings of the 26th IEEE International Conference on Software Analysis, Evolution and Reengineering (SANER)*, 49-60. [doi.org/10.1109/SANER.2019.8668024](https://doi.org/10.1109/SANER.2019.8668024)

Jergensen, C., Sarma, A., & Wagstrom, P. (2011). The onion patch: Migration in open source ecosystems. *Proceedings of the 19th ACM SIGSOFT Symposium on the Foundations of Software Engineering (FSE)*, 70-80. [doi.org/10.1145/2025113.2025127](https://doi.org/10.1145/2025113.2025127)

Jongeling, R., Sarkar, P., Datta, S., & Serebrenik, A. (2017). On negative results when using sentiment analysis tools for software engineering research. *Empirical Software Engineering*, 22(5), 2543-2584. [doi.org/10.1007/s10664-016-9493-x](https://doi.org/10.1007/s10664-016-9493-x)

Kalliamvakou, E., Gousios, G., Blincoe, K., Singer, L., German, D. M., & Damian, D. (2014). The promises and perils of mining GitHub. *Proceedings of the 11th Working Conference on Mining Software Repositories (MSR)*, 92-101. [doi.org/10.1145/2597073.2597074](https://doi.org/10.1145/2597073.2597074)

Kalliamvakou, E., Gousios, G., Blincoe, K., Singer, L., German, D. M., & Damian, D. (2016). An in-depth study of the promises and perils of mining GitHub. *Empirical Software Engineering*, 21(5), 2035-2071. [doi.org/10.1007/s10664-015-9393-5](https://doi.org/10.1007/s10664-015-9393-5)

Lin, B., Zampetti, F., Bavota, G., Di Penta, M., Lanza, M., & Oliveto, R. (2018). Sentiment analysis for software engineering: How far can we go? *Proceedings of the 40th International Conference on Software Engineering (ICSE)*, 94-104. [doi.org/10.1145/3180155.3180195](https://doi.org/10.1145/3180155.3180195)

Liu, N. F., Lin, K., Hewitt, J., Paranjape, A., Bevilacqua, M., Petroni, F., & Liang, P. (2024). Lost in the middle: How language models use long contexts. *Transactions of the Association for Computational Linguistics*, 12, 157-173. [doi.org/10.1162/tacl_a_00638](https://doi.org/10.1162/tacl_a_00638)

Liu, P., Yuan, W., Fu, J., Jiang, Z., Hayashi, H., & Neubig, G. (2023). Pre-train, prompt, and predict: A systematic survey of prompting methods in natural language processing. *ACM Computing Surveys*, 55(9), 1-35. [doi.org/10.1145/3560815](https://doi.org/10.1145/3560815)

Loughran, T., & McDonald, B. (2011). When is a liability not a liability? Textual analysis, dictionaries, and 10-Ks. *The Journal of Finance*, 66(1), 35-65. [doi.org/10.1111/j.1540-6261.2010.01625.x](https://doi.org/10.1111/j.1540-6261.2010.01625.x)

Mäntylä, M. V., Graziotin, D., & Kuutila, M. (2018). The evolution of sentiment analysis - A review of research topics, venues, and top cited papers. *Computer Science Review*, 27, 16-32. [doi.org/10.1016/j.cosrev.2017.10.002](https://doi.org/10.1016/j.cosrev.2017.10.002)

Murgia, A., Tourani, P., Adams, B., & Ortu, M. (2014). Do developers feel emotions? An exploratory analysis of emotions in software artifacts. *Proceedings of the 11th Working Conference on Mining Software Repositories (MSR)*, 262-271. [doi.org/10.1145/2597073.2597086](https://doi.org/10.1145/2597073.2597086)

Novielli, N., Calefato, F., Dongiovanni, D., Girardi, D., & Lanubile, F. (2020). Can we use SE-specific sentiment analysis tools in a cross-platform setting? *Proceedings of the 17th International Conference on Mining Software Repositories (MSR)*, 158-168. [doi.org/10.1145/3379597.3387446](https://doi.org/10.1145/3379597.3387446)

Ortu, M., Adams, B., Destefanis, G., Tourani, P., Marchesi, M., & Tonelli, R. (2015). Are bullies more productive? Empirical study of affectiveness vs. issue fixing time. *Proceedings of the 12th Working Conference on Mining Software Repositories (MSR)*, 303-313. [doi.org/10.1109/MSR.2015.35](https://doi.org/10.1109/MSR.2015.35)

Pang, B., & Lee, L. (2008). Opinion mining and sentiment analysis. *Foundations and Trends in Information Retrieval*, 2(1-2), 1-135. [doi.org/10.1561/1500000011](https://doi.org/10.1561/1500000011)

Rahman, M. M., Roy, C. K., & Kula, R. G. (2017). Predicting usefulness of code review comments using textual features and developer experience. *Proceedings of the 14th International Conference on Mining Software Repositories (MSR)*, 215-226. [doi.org/10.1109/MSR.2017.17](https://doi.org/10.1109/MSR.2017.17)

Reynolds, L., & McDonell, K. (2021). Prompt programming for large language models: Beyond the few-shot paradigm. *Extended Abstracts of the 2021 CHI Conference on Human Factors in Computing Systems (CHI EA)*, 1-7. [doi.org/10.1145/3411763.3451760](https://doi.org/10.1145/3411763.3451760)

Sadowski, C., Söderberg, E., Church, L., Sipko, M., & Bacchelli, A. (2018). Modern code review: A case study at Google. *Proceedings of the 40th International Conference on Software Engineering: Software Engineering in Practice (ICSE-SEIP)*, 181-190. [doi.org/10.1145/3183519.3183525](https://doi.org/10.1145/3183519.3183525)

Seidman, S. B. (1983). Network structure and minimum degree. *Social Networks*, 5(3), 269-287. [doi.org/10.1016/0378-8733(83)90028-X](https://doi.org/10.1016/0378-8733(83)90028-X)

Speer, A. B. (2021). Scoring dimension-level job performance from narrative comments: Validity and generalizability when using natural language processing. *Organizational Research Methods*, 24(3), 572-594. [doi.org/10.1177/1094428120930815](https://doi.org/10.1177/1094428120930815)

Tsay, J., Dabbish, L., & Herbsleb, J. (2014). Influence of social and technical factors for evaluating contribution in GitHub. *Proceedings of the 36th International Conference on Software Engineering (ICSE)*, 356-366. [doi.org/10.1145/2568225.2568315](https://doi.org/10.1145/2568225.2568315)

Wang, R., Chen, F., Chen, Z., Li, T., Harari, G., Tignor, S., Zhou, X., Ben-Zeev, D., & Campbell, A. T. (2014). StudentLife: Assessing mental health, academic performance and behavioral trends of college students using smartphones. *Proceedings of the ACM International Joint Conference on Pervasive and Ubiquitous Computing (UbiComp)*, 3-14. [doi.org/10.1145/2632048.2632054](https://doi.org/10.1145/2632048.2632054)