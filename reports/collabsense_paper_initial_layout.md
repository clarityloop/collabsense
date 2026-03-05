# CollabSense: Evaluating Enterprise AI Coaching Models on Open Source Collaboration Data

**Status:** Work in Progress  
**Project:** ClarityLoop / CollabSense  
**Focus:** AI Sentiment Analysis, Behavioral Coaching, and Graph Density in Open Source

---

## 1. Abstract
> *Brief overview of the project. Highlight the successful deployment of the ClarityLoop engine on public open-source data. Emphasize the model's high accuracy in mapping sentiment and identifying positive "Strengths." Summarize how combining an "Individual Signal" (medium) prompt with engineered "Artificial Density" successfully allowed the engine to generate accurate "Growth Opportunities."*

## 2. Introduction
> *Define the ClarityLoop engine's primary purpose (Enterprise behavioral coaching). State the research goal: testing the transferability of the AI to asynchronous, open-source environments (Pandas and Kubernetes). Establish that open-source code reviews are being used as a proxy for enterprise peer reviews.*

## 3. Literature Review
> *Review existing research to establish the baseline of what has been done in this field.*

### 3.1 Sentiment Analysis in the Workplace
> *Research on how NLP and LLMs are used to track morale, tone, and toxicity in corporate communications (Slack, Teams, Email).*

### 3.2 Automated Feedback & Growth Signals
> *Explore existing tools or papers focused on automated performance reviews, behavioral coaching, or soft-skill extraction from text.*

### 3.3 The Gap: Enterprise vs. Open Source Topology
> *Research comparing the social graphs of traditional companies (dense, hierarchical) versus open-source projects (sparse, transactional, "drive-by" contributions). How does this structural difference affect AI analysis?*

## 4. Methodology & Data Engineering
> *Detail the pipeline used to gather, clean, and process the data before feeding it to the AI.*

### 4.1 Dataset Selection & Anonymization
> *Explain the choice of `pandas-dev/pandas` and `kubernetes/kubernetes`. Mention the use of Faker to anonymize users and simulate a private enterprise workspace.*

### 4.2 Engineering "Artificial Density" (K-Core)
> *Explain the mathematical approach to solving open-source sparsity. Detail the use of K-Core decomposition to find the "Densest Subgraph" (e.g., the 55-person core Kubernetes team) to artificially recreate an Enterprise Department topology.*

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