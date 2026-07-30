# ModBench (SAM2026)

This repository contains the implementation of **ModBench**, the
Modelica benchmark-generation pipeline described in the SAM 2026 paper
*"ModBench: A Pipeline for Building Modelica Benchmark Datasets Mined
from Library Repositories"*.

It produces the `ModBench-MSL` corpus from the Modelica Standard
Library Git history and exposes a read-only access API over the
resulting artifacts.

## Repository layout

```
SAM2026/
├── pipeline/              # Generation pipeline (steps 1--3) + config
│   ├── settings.py        # Pipeline configuration & DB helpers
│   ├── 0_cleanup_data.py
│   ├── 1_filter_commits.py
│   ├── 2_extract_simulation_eligible_classes.py
│   ├── 3_build_canonical_representation.py
│   └── 3_build_canonical_representation_profile.py
├── reports.py             # Regenerates LaTeX macros and figures for the paper
├── dataset/               # Generated artifacts (the three stores)
│   ├── pipeline.db        # Step 1 + Step 3 tables (~1.2 GB on MSL)
│   ├── step2_classes.db   # Class-listing store (~12 GB on MSL)
│   └── canonical_models/  # Canonical .mo files (~199 GB on MSL)
├── access/                # Read-only API, notebook
│   ├── api.py
│   └── explore.ipynb
├── source/                # Cloned Modelica library repositories
│   ├── MSL/
│   ├── Buildings/
│   └── ...
└── worktrees/             # Per-worker git worktrees (runtime)
```

The three persistent stores in `dataset/` correspond directly to the
*pipeline DB*, *class-listing DB*, and *canonical files* described in
the paper (Section 4, Table 2).

## Prerequisites

- Python 3.10+
- [OpenModelica Compiler (OMC)](https://openmodelica.org/) `omc`
- Python packages from `requirements.txt`

Install Python dependencies:

```bash
pip install -r requirements.txt
```

### Local configuration (`.env`)

Deployment-specific values (GitHub token, optional SSH remote host /
base path used by `reports.py` and the canonical-file fallback) are
read from a local `.env` file. Copy the template and edit:

```bash
cp .env.example .env
```

`.env` is gitignored — keep credentials and host aliases out of version
control. All variables are optional; the pipeline runs entirely
locally when none are set.

## The generation pipeline

The pipeline is organised into three reproducible, resumable stages.
Each stage persists its results to a database table or an on-disk
directory.

```bash
# Step 1 — filter candidate revisions
python pipeline/1_filter_commits.py

# Step 2 — list Modelica classes
python pipeline/2_extract_simulation_eligible_classes.py

# Step 3 — build canonical representations
python pipeline/3_build_canonical_representation.py
```

Per-step report scripts (under `pipeline/`) and the top-level
`reports.py` regenerate the figures and LaTeX macros used in the paper
from the contents of `pipeline.db`.

### Reference runtimes (MSL)

The numbers below are wall-clock times measured on a full MSL run of
7,360 retained commits (after Step 1) using **OpenModelica 1.28.0**.
Steps 2 and 3 ran with **8 parallel worker processes**. The host is a
Dell PowerEdge R630 server running Ubuntu 24.04 LTS with 2× Intel Xeon
E5-2623 v3 CPUs (8 physical cores / 16 threads total at 3.00 GHz, up
to 3.50 GHz boost) and 64 GB of RAM.

| # | Step                                | Wall-clock | Throughput            |
|---|-------------------------------------|-----------:|-----------------------|
| 1 | Filter Commits & Files              | 62 s       | 163.53 commits/s      |
| 2 | Extract Simulation-Eligible Classes | 13 h 06 m (1 h 18 m) | 6.38 s/commit (0.64) |
| 3 | Build Canonical Representation      | 62 h 43 m  | 30.68 s/commit        |
| **Σ** | **Total**                       | **≈ 75 h 50 m** (≈ 64 h 02 m) |        |

Step 2 writes each commit's class index to `cache/step2_class_index/` and reuses
it on a later run, which skips the per-class `isExperiment` query that dominates
enumeration. The wall-clock figures are for a cold cache — what a first run on a
library costs. The figures in parentheses are a re-run with that cache warm.

#### What a canonical representation excludes (Step 3)

A canonical representation must be comparable across a library's history: two
snapshots of a class should differ only when the class itself does. Everything
with no effect on compilation or simulation is therefore removed while the model
is still inside the compiler — no saved file is ever edited afterwards.

**Description strings** go during the save. `saveTotalModel` is called with
`stripComments = true`, which removes every description string — on classes,
components, function arguments and enumeration literals. The flag costs nothing
measurable, so it is not configurable. The save also removes, on its own:
graphical annotations (`Icon`, `Diagram`, `Placement`, `Line`), `Documentation`,
source comments, and part of the library metadata (`uses`, `conversion`,
`revisionId`, `preferredView`, `defaultComponentName`).

The companion flag `stripAnnotations` is deliberately **not** used: it would also
remove the annotations a compiler acts on, including the `experiment` annotation
that marks a class simulation-eligible.

**Library release metadata** goes once per commit, right after the library is
loaded and before any class is saved. `version`, `versionDate`, `versionBuild` and
`dateModified` sit on a library's top-level packages and change whenever a release
is cut, so left in place they make **every** model in the library look modified at
every version bump — the exact signal a history study is trying to measure.
`clean_loaded_library()` rewrites those package annotations in the AST the compiler
holds, through `getElementAnnotation` / `setElementAnnotation`, keeping every entry
that is semantic or not yet classified. It costs ~0.25 s per commit, against ~27 s
to save a 40-class commit.

##### Worked example

This model carries every non-semantic annotation that survives a `saveTotalModel`
call, plus descriptions, graphics and `Documentation`:

```modelica
package AnnotationSurvivors "Removed: package description"
  // Removed: source comment.
  annotation(
    version = "1.2.3", versionDate = "2026-07-30", versionBuild = 4,
    dateModified = "2026-07-30 12:00:00Z", revisionId = "R1",
    uses(Modelica(version = "4.0.0")), conversion(noneFromVersion = "1.0.0"),
    preferredView = "info",
    Documentation(info = "<html>Removed: Documentation.</html>"));

  type Angle = Real(unit = "rad") "Removed: type description"
    annotation(absoluteValue = true);

  record Settings "Removed: record description"
    parameter Real gain = 2 "Removed: parameter description";
    annotation(defaultComponentPrefixes = "parameter", defaultComponentName = "settings");
  end Settings;

  partial model Base "Removed: base description"
    Real y "Removed: variable description";
  end Base;

  model Example "Removed: model description"
    extends Base annotation(IconMap(primitivesVisible = false));
    Settings settings annotation(choicesAllMatching = true, __Dymola_editText = false);
    Angle phi "Removed: component description"
      annotation(Placement(transformation(extent = {{-10,-10},{10,10}})));
  equation
    y = settings.gain * time;
    phi = y;
    annotation(
      experiment(StopTime = 2, Tolerance = 1e-6),
      __OpenModelica_commandLineOptions = "-d=initialization",
      __Dymola_Commands(file = "plotResults.mos"),
      Icon(graphics = {Rectangle(extent = {{-100,-100},{100,100}})}),
      Diagram(coordinateSystem(extent = {{-100,-100},{100,100}})));
  end Example;
end AnnotationSurvivors;
```

Step 3 turns it into:

```modelica
package AnnotationSurvivors
  type Angle = Real(unit = "rad") annotation(absoluteValue = true);

  record Settings
    parameter Real gain = 2;
    annotation(defaultComponentPrefixes = "parameter");
  end Settings;

  partial model Base
    Real y;
  end Base;

  model Example
    extends Base annotation(IconMap(primitivesVisible = false));
    Settings settings annotation(choicesAllMatching = true, __Dymola_editText = false);
    Angle phi;
  equation
    y = settings.gain*time;
    phi = y;
    annotation(experiment(StopTime = 2, Tolerance = 1e-6), __OpenModelica_commandLineOptions = "-d=initialization", __Dymola_Commands(file = "plotResults.mos"));
  end Example;
end AnnotationSurvivors;

model Example_total
  extends AnnotationSurvivors.Example;
 annotation(experiment(StopTime = 2, Tolerance = 1e-6), __OpenModelica_commandLineOptions = "-d=initialization");
end Example_total;
```

Gone: every description string, the source comment, `Documentation`, the graphics,
and the package's `version`, `versionDate`, `versionBuild`, `dateModified`,
`revisionId`, `uses`, `conversion` and `preferredView`. Kept: `experiment` and
`__OpenModelica_commandLineOptions`, which the compiler reads.

What remains and is not semantic — `absoluteValue`, `defaultComponentPrefixes`,
`IconMap`, `choicesAllMatching`, `__Dymola_*` — sits on individual classes,
components and extends clauses rather than on the package. Reaching those would
mean reading the annotation of every class in the library, ~15 ms per element or
about 90 s for MSL's 6,373 classes, several times the cost of saving a whole
commit. They are also identical in every commit, so they never make a class look
modified; only a library edit to one of them shows up, and that is a real source
change.

#### Pilot mode (Step 3)

Step 3 supports an optional **pilot** mode that restricts canonicalization
to a curated whitelist of class names rather than every experiment class
found in Step 2. This is useful for fast iteration and for reproducing a
small benchmark subset without compiling the entire library history.

* The whitelist lives in the `step3_sublibraries` table of `pipeline.db`
  and is seeded from `DEFAULT_PILOT_SUBLIBRARIES` in `settings.py`.
* Pilot mode is controlled by the `PILOT_ENABLED` and `PILOT_ALLOW_PREFIX`
  flags in `run_settings` (step 3). When `PILOT_ALLOW_PREFIX` is on, a
  whitelist entry matches any class whose fully-qualified name starts
  with the entry.
* The `step3_classes` table records, per class snapshot, whether the
  class was inside the whitelist (`is_inside_sublibraries_list`), which
  matching mode applied (`pilot_match_mode`), and which whitelist entry
  it matched (`matched_sublibrary`). The on-disk canonical outputs and
  the schema columns described in the paper (`canonical_model_path`,
  `canonical_produced`, `error_message`) are populated identically in
  both modes.
* To disable pilot mode and canonicalize every experiment class, set
  `PILOT_ENABLED = 0` in the `run_settings` table for step 3.

### Running on a different Modelica library

1. Clone the repository under `source/<name>/`.
2. Add an entry to the `SOURCES` table in `pipeline.db`
   (see `DEFAULT_SOURCES` in `settings.py`) and set `enabled = 1`.
3. Re-run the pipeline; only the new source will be processed.

## The dataset

Running the pipeline yields three persistent stores under
`dataset/` (see paper Section 4):

| Store              | File / directory             | Contents                                |
|--------------------|------------------------------|-----------------------------------------|
| Pipeline DB        | `dataset/pipeline.db`        | `step1_*` and `step3_*` tables          |
| Class-listing DB   | `dataset/step2_classes.db`   | `step2_classes`                         |
| Canonical files    | `dataset/canonical_models/`  | One `.mo` file per simulation-eligible class snapshot |

## Access API

`access/api.py` provides a read-only `ModelicaDataset` class for
querying the dataset without writing SQL:

```python
from access.api import ModelicaDataset

with ModelicaDataset() as ds:
    classes = ds.list_experiment_classes("MSL")
    timeline = ds.get_class_timeline("MSL", classes[0])
    for snap in timeline:
        src = ds.read_canonical_model(snap.canonical_model_path)
        # ... analyse `src`
    failures = ds.list_canonicalization_failures("MSL")
```

Capabilities:

- list source libraries, commits, and experiment classes;
- retrieve all canonicalised class versions in commit order;
- list all models associated with a commit;
- read canonical source for a specific class at a specific revision;
- inspect recorded canonicalisation failures;
- fetch GitHub commit / pull request / issue metadata.

Open `access/explore.ipynb` for an interactive walkthrough.

### Cleanup

```bash
python pipeline/0_cleanup_data.py        # interactive
python pipeline/0_cleanup_data.py --yes  # non-interactive
```
