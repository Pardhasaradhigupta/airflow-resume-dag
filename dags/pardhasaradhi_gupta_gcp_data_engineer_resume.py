from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


def log_section(title: str, content: str):
    line = "=" * 110
    print(f"\n{line}")
    print(title)
    print("-" * 110)
    print(content.strip())
    print(f"{line}\n")


with DAG(
    dag_id="pardhasaradhi_gupta_gcp_data_engineer_resume",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["resume", "gcp", "airflow", "portfolio"],
) as dag:

############ 1. Professional Summary ######################
    summary = PythonOperator(
        task_id="01_professional_summary",
        python_callable=log_section,
        op_args=[
            "PROFESSIONAL SUMMARY",
            """
GCP-certified Data Engineer with 3+ years of expertise in building scalable ETL/ELT pipelines,
optimizing BigQuery, and automating orchestration using Apache Airflow (Cloud Composer).

Proficient in infrastructure-as-code with Terraform, CI/CD implementation, and data governance.
Holds Google Cloud Professional Data Engineer, Professional Cloud Architect, and Generative AI
Engineer certifications.

Skilled at solving complex data challenges with performance tuning, cost optimization,
and proactive monitoring.
            """,
        ],
    )

  ################# 2. Core Competencies ################
    skills = PythonOperator(
        task_id="02_core_competencies",
        python_callable=log_section,
        op_args=[
            "CORE COMPETENCIES",
            """
Cloud & Data Engineering
- GCP (BigQuery, Cloud Storage, Pub/Sub, Dataflow, Cloud Functions, Cloud Composer)

Orchestration & ETL
- Airflow DAGs, ETL / ELT pipelines, cross-project modularization

Programming & Scripting
- Python, SQL

Infra as Code & DevOps
- Terraform, CI/CD (Concourse, Git), automated deployments

Cost & Performance Optimization
- Partitioning, clustering, materialized views, query tuning

Monitoring & Alerting
- Airflow alerts, post-deployment validation

Governance & Security
- Persona-based access control, tag-based resource access, audit logging
            """,
        ],
    )

  ######## 3. Professional Experience ###############
    with TaskGroup(group_id="03_professional_experience") as experience:

        role = PythonOperator(
            task_id="01_role_context",
            python_callable=log_section,
            op_args=[
                "PROFESSIONAL EXPERIENCE",
                """
Role: Data Eng, Mgmt & Governance Analyst
Organization: Accenture
Location: Hyderabad
Duration: APR 2023 – Present
                """,
            ],
        )

        impact = PythonOperator(
            task_id="02_key_responsibilities",
            python_callable=log_section,
            op_args=[
                "KEY RESPONSIBILITIES & IMPACT",
                """
- Upgraded and optimized multiple Cloud Composer (Airflow) instances, improving orchestration
  reliability across business departments.

- Implemented email-based automation alerts for persistent CI/CD pipeline failures, cutting
  incident resolution time by 80%.

- Re-architected Airflow deployment using modular Composer instances and Pub/Sub for
  cross-project DAG communication, reducing slot usage and costs.

- Partnered with Data Platform Engineering to enhance CI/CD pipelines, supporting architecture
  changes and deployment velocity.

- Managed data infrastructure through Terraform, enabling high availability and rapid
  provisioning of GCP resources.

- Troubleshot production DAGs and achieved faster recovery by optimizing pipeline health checks.

- Delivered a GenAI-powered log analysis POC in BigQuery, enabling faster root cause
  identification.

- Executed BigQuery cost optimization using clustering, partitioning, materialized views,
  and query rewrites to enhance performance and reduce costs.
                """,
            ],
        )

        role >> impact

    ###### 4. Certifications & Achievements ########
    certifications = PythonOperator(
        task_id="04_certifications_and_achievements",
        python_callable=log_section,
        op_args=[
            "CERTIFICATIONS & ACHIEVEMENTS",
            """
Certifications
- Google Cloud Professional Data Engineer
- Google Cloud Professional Cloud Architect
- Google Cloud Generative AI Engineer
- Google Cloud Associate Cloud Engineer

Achievements
- Received SPOT Award at Accenture for 2 consecutive years, recognizing exceptional performance,
  commitment, and contributions to team success.
            """,
        ],
    )

    ########## DAG Flow ###########
  
    summary >> skills >> experience >> certifications
