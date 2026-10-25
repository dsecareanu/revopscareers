# Role read for candidates: methodology

Draft, 2026-10-25. A short, candidate-focused read of a job ad, shown on featured
Revenue Operations jobs on RevOps Careers, right under the job description and above
"Tagged as". It links to ReLoop.me's full Role Intel for the complete read.

## Sources

The method combines three things:

1. ReLoop.me's public material:
   - The [methodology page](https://www.reloop.me/methodology). Most of the scoring logic is proprietary and not published.
   - The [Fair Read Charter](https://www.reloop.me/charter) (CC BY 4.0, published by ZORRI S.R.L.).
   - [How to read a job description](https://www.reloop.me/resources/how-to-read-a-job-description).
2. The structure of a real Role Intel brief (briefs.reloop.me/yXAlV5d, Oct 2026), keeping only its candidate-facing parts.
3. RevOps Careers' own 2026 job data for typical years of experience and pay by level.

We do not reuse ReLoop.me's proprietary ladders, weights or pay tables. We use their published reading approach and their charter rules, and say so on the page.

## Reading rules (from the Fair Read Charter, adapted to roles)

1. **Evidence over titles.** The title is a claim; the duties are the evidence. "The title is the costume. The scope is the skeleton."
2. **Unknown is not zero.** If the ad doesn't say (team size, pay, reporting line), the read says "not stated". It never guesses or fills in an average.
3. **Describe, don't rank.** The read describes the job. It never scores or ranks candidates or employers.
4. **Show the why.** Every judgement carries a short reason and, where possible, a quote from the ad.
5. **Inferred is labelled inferred.** Anything not stated outright is marked as the model's reading.

## What the read covers (candidate view only)

Taken from the public "6 things to decode" and the candidate half of the example brief.

| Section | What it says | Source in the ad |
|---|---|---|
| The job in plain words | 1–2 sentences: what you will actually spend your time on | Duties |
| Real level vs title | Level the work implies (Entry, Mid, Senior IC, Lead, Manager, Senior Manager, Director, Head/VP), whether you'd manage people, and whether the title matches. Plain wording such as "Titled Strategist; reads as a hands-on senior HubSpot specialist" | Authority, autonomy, reports, decisions |
| Must-haves vs nice-to-haves | Up to 5 load-bearing requirements and up to 5 preferences, each with a short quote from the ad | Requirements |
| Things to check before applying | Up to 3 neutral flags, only when the ad shows them: one-person function, several jobs in one, title broader or narrower than the work, unclear success measures, requirements unusually broad for the level | Whole ad |
| How the job leans | 4 scales, each with a one-line reason: hands-on vs strategic, maintain vs build, specialist vs generalist, clear vs open-ended scope | Duties |
| AI in the role | None / uses AI tools / builds AI workflows, with the quote | Duties, tools |
| Worth applying if you can show... | 2–3 concrete things to prove on your resume or in the interview | Must-haves |
| Questions to ask | 3 questions that fill gaps the ad leaves open (team, reporting line, success in 90 days, pay if missing) | Gaps |

### Context from RevOps Careers data (computed, not from the AI model)

- **Pay:** whether the ad states a range. If it does and the role is US-based, where its midpoint sits against the RevOps Careers 2026 median for that level. Example: "$75k–$80k; the median for US Manager ads in 2026 is $127k". We never estimate pay that isn't stated.
- **Experience:** years asked against the 2026 median for the level (Analyst 3, Manager 5, Director 7, VP 8).

### Deliberately left out

- **Pay estimates.** They would need ReLoop.me's calibrated tables, and Ligia flagged them as error-prone.
- **The hiring-manager half of the brief** (job-ad fixes, sourcing, ideal profiles) and employer-risk or "high inflation" labels. Candidates don't need these, and they read as criticism on employers' pages.
- **Any match against a candidate's resume,** and any ranking of candidates.

## Which jobs get a read

- Revenue Operations (category 21) jobs that are featured at import time: about 40 a day.
- Only jobs whose description is at least 1,500 characters. Short summaries give the model too little to read.
- Paid client jobs (author other than webadmin): off by default, or opt-in per client. Open decision.
- One read per job, stored. It is not refreshed unless the ad changes.

## How it runs

1. Runs as a step in the daily sync after the imports, in Python on the tig server.
2. Calls Claude Haiku 5.5 (`claude-haiku-5-5`) through the Anthropic Python SDK, with a structured JSON output schema and effort `low`. One request per job, with the job title, company, location and plain-text description.
3. Validation in code, before saving:
   - Every quote must appear verbatim in the ad; otherwise the item is dropped.
   - The level must be one of the fixed ladder values.
   - Empty or "not stated" values stay empty and are shown as "Not stated".
4. Stored in post meta `_roc_role_read` (JSON + model + date + description hash).
5. A PHP block in the child theme renders it under the description, above "Tagged as", only when that meta exists.

Cost estimate: about 1.5k input and 1k output tokens per job. At about 40 jobs a day that's roughly $0.03 a day, or about $1 a month.

## How it's labelled on the page

- **Heading:** see the naming decision below.
- **Line under the heading:** "AI-generated read of this job ad by RevOps Careers, using Claude. Indicative, not career advice. Based on ReLoop.me's published Fair Read method."
- **Call to action:** "Want the full read, with an estimated pay range? Get a ReLoop.me Role Intel →", linking to reloop.me/role-intel with UTM tags.
- **Footer:** generation date, plus "Something wrong? Tell us" (mailto).

### Naming (open decision)

- Calling our output "Sample ReLoop.me Role Intel" would present text we generated as ReLoop.me's product. Readers would credit or blame ReLoop.me for reads they didn't make.
- Use that name only with Ligia's written OK.
- Otherwise use: **"Role read, based on ReLoop.me's Role Intel method"** (or "Quick role read"), with ReLoop.me named and linked in the subtitle and the call to action.

## Known limits

- The level reading is the least stable part. ReLoop.me reports the same issue: in their tests, seniority moved between Senior Manager and Director across runs.
- Reads are only as good as the ad. Thin ads get "not stated" rather than guesses.
- No human review per job. Spot-check the first 20 reads before turning it on site-wide.
