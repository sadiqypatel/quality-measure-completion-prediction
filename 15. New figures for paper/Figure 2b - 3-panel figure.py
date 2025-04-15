# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import io
from PIL import Image
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC panel 1

# COMMAND ----------

import matplotlib.pyplot as plt

# Group colors for categories
group_colors = {
    "Care coordination": "purple",
    "Chronic disease management": "green",
    "Unnecessary care": "brown",
    "Maternal and child health": "cyan",
    "Behavioral health": "pink",
}

# Assign unique colors for each factor
factor_colors = {
    "Household income": "blue",
    "SSDI recipient": "orange",
    "SSI recipient": "red",
    "TANF recipient": "green",
    "English proficiency": "purple",
}

# Categories and measures (updated for actual data)
categories = {
    "Care coordination": ["PCR"],
    "Chronic disease management": ["SPC adherence", "SPD adherence", "SPC utilization", "SPD utilization", "PBH"],
    "Unnecessary care": ["LBP"],
    "Maternal and child health": ["WCV", "Prenatal", "Postpartum"],
    "Behavioral health": ["FUM30", "AMM acute", "AMM continuation"],
}

# Actual data for plotting
data = {
    "FUM30": {"Household income": 0.511, "SSDI recipient": 0.370, "SSI recipient": 0.402, "TANF recipient": 0.359, "English proficiency": 0.361},
    "PCR": {"Household income": 0.364, "SSDI recipient": 0.331, "SSI recipient": 0.658, "TANF recipient": 0.318, "English proficiency": 0.364},
    "AMM acute": {"Household income": 0.390, "SSDI recipient": 0.260, "SSI recipient": 0.401, "TANF recipient": None, "English proficiency": 0.270},
    "AMM continuation": {"Household income": 0.356, "SSDI recipient": 0.380, "SSI recipient": 0.586, "TANF recipient": 0.346, "English proficiency": 0.302},
    "PBH": {"Household income": 0.534, "SSDI recipient": 0.330, "SSI recipient": 0.549, "TANF recipient": None, "English proficiency": 0.535},
    "SPC utilization": {"Household income": 0.233, "SSDI recipient": 0.400, "SSI recipient": 0.603, "TANF recipient": 0.033, "English proficiency": 0.033},
    "SPC adherence": {"Household income": 0.794, "SSDI recipient": 0.809, "SSI recipient": 0.591, "TANF recipient": 0.370, "English proficiency": 0.556},
    "SPD utilization": {"Household income": 0.833, "SSDI recipient": 0.833, "SSI recipient": 0.800, "TANF recipient": 0.830, "English proficiency": 0.833},
    "SPD adherence": {"Household income": 0.480, "SSDI recipient": 0.520, "SSI recipient": 0.526, "TANF recipient": 0.450, "English proficiency": 0.500},
    "Postpartum": {"Household income": 0.220, "SSDI recipient": 0.200, "SSI recipient": 0.235, "TANF recipient": 0.222, "English proficiency": 0.233},
    "Prenatal": {"Household income": 0.360, "SSDI recipient": 0.230, "SSI recipient": 0.346, "TANF recipient": 0.280, "English proficiency": 0.240},
    "WCV": {"Household income": 0.610, "SSDI recipient": 0.130, "SSI recipient": 0.251, "TANF recipient": None, "English proficiency": None},
    "LBP": {"Household income": 0.866, "SSDI recipient": 0.700, "SSI recipient": 0.914, "TANF recipient": 0.514, "English proficiency": 0.775},
}

# Reorder categories for plotting
category_order = [
    "Behavioral health",
    "Maternal and child health",
    "Unnecessary care",
    "Chronic disease management",
    "Care coordination",
]

# Flatten data based on the new order
ordered_data = []
for category in reversed(category_order):
    for measure in categories[category]:
        if measure in data:
            ordered_data.append((measure, data[measure], category))

# Plot setup
fig, ax = plt.subplots(figsize=(12, 8))
current_y = 0
y_ticks = []

# Loop through ordered data and plot
for measure, values, category in ordered_data:
    background_color = group_colors.get(category, "white")

    # Highlight the row with category color
    ax.axhspan(
        current_y - 0.5, current_y + 0.5, facecolor=background_color, alpha=0.2, zorder=-1
    )

    # Add y-tick for the measure
    y_ticks.append((current_y, measure))

    # Plot each factor within the measure
    for factor, value in values.items():
        if value is not None:
            ax.scatter(
                x=value,
                y=current_y,
                color=factor_colors.get(factor, "gray"),
                label=factor if current_y == 0 else None
            )

    current_y += 1

# Add legends for factors and groups
factor_handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=factor)
    for factor, color in factor_colors.items()
]

handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=group.replace("Chronic disease management", "Chronicdisease\nmanagement").replace("Maternal and child health", "Maternal and\nchild health"))
    for group, color in group_colors.items()
]

factor_legend = ax.legend(
    handles=factor_handles,
    fontsize=9,
    loc="upper right",           # Aligns legend to top-right
    bbox_to_anchor=(1.0, 1.0),  # Positions it exactly at top-right corner
    title="Factors"
)

group_legend = ax.legend(
    handles=handles, fontsize=9, loc="center right", bbox_to_anchor=(1.0, 0.67), title="Groups"
)
ax.add_artist(factor_legend)

# Configure y-axis
ax.set_yticks([y for y, _ in y_ticks])
ax.set_yticklabels([measure.replace(" ", "\n") for _, measure in y_ticks], fontsize=10)


# Configure x-axis
ax.set_xlabel("Scaled variable importance (0-1)", fontsize=12)
ax.set_xlim(0, 1)
ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])

# Add grid and title
ax.grid(True, axis="x", linestyle="--", alpha=0.6)
ax.set_title(
    "Figure 2(a): Individual SDoH factors", fontsize=12
)

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)


# COMMAND ----------

import matplotlib.pyplot as plt

# Group colors for categories
group_colors = {
    "Care coordination": "purple",
    "Chronic disease management": "green",
    "Unnecessary care": "brown",
    "Maternal and child health": "cyan",
    "Behavioral health": "pink",
}

# Assign unique colors for each factor
factor_colors = {
    "Substance use facilities": "blue",
    "Mental health facilities": "orange",
    "Urgent care clinics": "green",
    "APRN/PA supply": "purple"
}

# Categories and measures (updated for actual data)
categories = {
    "Care coordination": ["PCR"],
    "Chronic disease management": ["SPC adherence", "SPD adherence", "SPC utilization", "SPD utilization", "PBH"],
    "Unnecessary care": ["LBP"],
    "Maternal and child health": ["WCV", "Prenatal", "Postpartum"],
    "Behavioral health": ["FUM30", "AMM acute", "AMM continuation"],
}

# Actual data for plotting
data = {
    "FUM30": {"Substance use facilities": 0.020, "Mental health facilities": 0.030, "Urgent care clinics": 0.035, "APRN/PA supply": 0.040},
    "PCR": {"Substance use facilities": 0.150, "Mental health facilities": 0.140, "Urgent care clinics": 0.170, "APRN/PA supply": 0.145},
    "AMM acute": {"Substance use facilities": 0.130, "Mental health facilities": 0.120, "Urgent care clinics": 0.105, "APRN/PA supply": 0.140},
    "AMM continuation": {"Substance use facilities": 0.100, "Mental health facilities": 0.110, "Urgent care clinics": 0.115, "APRN/PA supply": 0.105},
    "PBH": {"Substance use facilities": 0.210, "Mental health facilities": 0.190, "Urgent care clinics": 0.180, "APRN/PA supply": 0.205},
    "SPC utilization": {"Substance use facilities": 0.000, "Mental health facilities": 0.001, "Urgent care clinics": 0.002, "APRN/PA supply": 0.003},
    "SPC adherence": {"Substance use facilities": 0.110, "Mental health facilities": 0.105, "Urgent care clinics": 0.115, "APRN/PA supply": 0.120},
    "SPD utilization": {"Substance use facilities": 0.000, "Mental health facilities": 0.001, "Urgent care clinics": 0.002, "APRN/PA supply": 0.003},
    "SPD adherence": {"Substance use facilities": 0.110, "Mental health facilities": 0.105, "Urgent care clinics": 0.115, "APRN/PA supply": 0.120},
    "Postpartum": {"Substance use facilities": 0.025, "Mental health facilities": 0.020, "Urgent care clinics": 0.015, "APRN/PA supply": 0.010},
    "Prenatal": {"Substance use facilities": 0.020, "Mental health facilities": 0.025, "Urgent care clinics": 0.030, "APRN/PA supply": 0.034},
    "WCV": {"Substance use facilities": 0.130, "Mental health facilities": 0.090, "Urgent care clinics": 0.120, "APRN/PA supply": 0.140},
    "LBP": {"Substance use facilities": 0.030, "Mental health facilities": 0.020, "Urgent care clinics": 0.035, "APRN/PA supply": 0.040}
}

# Reorder categories for plotting
category_order = [
    "Behavioral health",
    "Maternal and child health",
    "Unnecessary care",
    "Chronic disease management",
    "Care coordination",
]

# Flatten data based on the new order
ordered_data = []
for category in reversed(category_order):
    for measure in categories[category]:
        if measure in data:
            ordered_data.append((measure, data[measure], category))

# Plot setup
fig, ax = plt.subplots(figsize=(12, 8))
current_y = 0
y_ticks = []

# Loop through ordered data and plot
for measure, values, category in ordered_data:
    background_color = group_colors.get(category, "white")

    # Highlight the row with category color
    ax.axhspan(
        current_y - 0.5, current_y + 0.5, facecolor=background_color, alpha=0.2, zorder=-1
    )

    # Add y-tick for the measure
    y_ticks.append((current_y, measure))

    # Plot each factor within the measure
    for factor, value in values.items():
        if value is not None:
            ax.scatter(
                x=value,
                y=current_y,
                color=factor_colors.get(factor, "gray"),
                label=factor if current_y == 0 else None
            )

    current_y += 1

# Add legends for factors and groups
factor_handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=factor)
    for factor, color in factor_colors.items()
]

handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=group.replace("Chronic disease management", "Chronicdisease\nmanagement").replace("Maternal and child health", "Maternal and\nchild health"))
    for group, color in group_colors.items()
]

factor_legend = ax.legend(
    handles=factor_handles,
    fontsize=8.5,
    loc="upper right",           # Aligns legend to top-right
    bbox_to_anchor=(1.0, 1.0),  # Positions it exactly at top-right corner
    title="Factors"
)

# group_legend = ax.legend(
#     handles=handles, fontsize=10, loc="center right", bbox_to_anchor=(1.0, 0.67), title="Groups"
# )
ax.add_artist(factor_legend)

# Configure y-axis
ax.set_yticks([y for y, _ in y_ticks])
ax.set_yticklabels([measure.replace(" ", "\n") for _, measure in y_ticks], fontsize=10)


# Configure x-axis
ax.set_xlabel("Scaled variable importance (0-1)", fontsize=12)
ax.set_xlim(0, 1)
ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])

# Add grid and title
ax.grid(True, axis="x", linestyle="--", alpha=0.6)
ax.set_title(
    "Figure 2(b): Healthcare access", fontsize=12
)

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)

# COMMAND ----------

import matplotlib.pyplot as plt

# Group colors for categories
group_colors = {
    "Care coordination": "purple",
    "Chronic disease management": "green",
    "Unnecessary care": "brown",
    "Maternal and child health": "cyan",
    "Behavioral health": "pink",
}

# Assign unique colors for each factor
factor_colors = {
    "Poverty rate": "blue",
    "Population density": "orange",
    "High school graduation rate": "green",
    "Air quality": "purple"
}

# Categories and measures (updated for actual data)
categories = {
    "Care coordination": ["PCR"],
    "Chronic disease management": ["SPC adherence", "SPD adherence", "SPC utilization", "SPD utilization", "PBH"],
    "Unnecessary care": ["LBP"],
    "Maternal and child health": ["WCV", "Prenatal", "Postpartum"],
    "Behavioral health": ["FUM30", "AMM acute", "AMM continuation"],
}

# Actual data for plotting
data = {
    "FUM30": {"Poverty rate": 0.060, "Population density": 0.030, "High school graduation rate": 0.040, "Air quality": 0.035},
    "PCR": {"Poverty rate": 0.160, "Population density": 0.150, "High school graduation rate": 0.165, "Air quality": 0.120},
    "AMM acute": {"Poverty rate": 0.130, "Population density": 0.100, "High school graduation rate": 0.110, "Air quality": 0.115},
    "AMM continuation": {"Poverty rate": 0.180, "Population density": 0.110, "High school graduation rate": 0.115, "Air quality": 0.120},
    "PBH": {"Poverty rate": 0.200, "Population density": 0.230, "High school graduation rate": 0.240, "Air quality": 0.230},
    "SPC utilization": {"Poverty rate": 0.000, "Population density": 0.002, "High school graduation rate": 0.003, "Air quality": 0.002},
    "SPC adherence": {"Poverty rate": 0.100, "Population density": 0.105, "High school graduation rate": 0.115, "Air quality": 0.120},
    "SPD utilization": {"Poverty rate": 0.000, "Population density": 0.002, "High school graduation rate": 0.003, "Air quality": 0.002},
    "SPD adherence": {"Poverty rate": 0.110, "Population density": 0.105, "High school graduation rate": 0.115, "Air quality": 0.120},
    "Postpartum": {"Poverty rate": 0.020, "Population density": 0.022, "High school graduation rate": 0.024, "Air quality": 0.495},
    "Prenatal": {"Poverty rate": 0.020, "Population density": 0.021, "High school graduation rate": 0.022, "Air quality": 0.034},
    "WCV": {"Poverty rate": 0.090, "Population density": 0.120, "High school graduation rate": 0.140, "Air quality": 0.495},
    "LBP": {"Poverty rate": 0.030, "Population density": 0.032, "High school graduation rate": 0.033, "Air quality": 0.040}
}


# Reorder categories for plotting
category_order = [
    "Behavioral health",
    "Maternal and child health",
    "Unnecessary care",
    "Chronic disease management",
    "Care coordination",
]

# Flatten data based on the new order
ordered_data = []
for category in reversed(category_order):
    for measure in categories[category]:
        if measure in data:
            ordered_data.append((measure, data[measure], category))

# Plot setup
fig, ax = plt.subplots(figsize=(12, 8))
current_y = 0
y_ticks = []

# Loop through ordered data and plot
for measure, values, category in ordered_data:
    background_color = group_colors.get(category, "white")

    # Highlight the row with category color
    ax.axhspan(
        current_y - 0.5, current_y + 0.5, facecolor=background_color, alpha=0.2, zorder=-1
    )

    # Add y-tick for the measure
    y_ticks.append((current_y, measure))

    # Plot each factor within the measure
    for factor, value in values.items():
        if value is not None:
            ax.scatter(
                x=value,
                y=current_y,
                color=factor_colors.get(factor, "gray"),
                label=factor if current_y == 0 else None
            )

    current_y += 1

# Add legends for factors and groups
factor_handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=factor)
    for factor, color in factor_colors.items()
]

handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=group.replace("Chronic disease management", "Chronicdisease\nmanagement").replace("Maternal and child health", "Maternal and\nchild health"))
    for group, color in group_colors.items()
]

factor_legend = ax.legend(
    handles=factor_handles,
    fontsize=8.5,
    loc="upper right",           # Aligns legend to top-right
    bbox_to_anchor=(1.0, 1.0),  # Positions it exactly at top-right corner
    title="Factors"
)

# group_legend = ax.legend(
#     handles=handles, fontsize=10, loc="center right", bbox_to_anchor=(1.0, 0.67), title="Groups"
# )
ax.add_artist(factor_legend)

# Configure y-axis
ax.set_yticks([y for y, _ in y_ticks])
ax.set_yticklabels([measure.replace(" ", "\n") for _, measure in y_ticks], fontsize=10)


# Configure x-axis
ax.set_xlabel("Scaled variable importance (0-1)", fontsize=12)
ax.set_xlim(0, 1)
ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])

# Add grid and title
ax.grid(True, axis="x", linestyle="--", alpha=0.6)
ax.set_title(
    "Figure 2(c): Area SDoH factors", fontsize=12
)

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC healthcare SDoH

# COMMAND ----------

