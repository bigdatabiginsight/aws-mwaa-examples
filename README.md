# AWS MWAA Examples

Example DAGs and use cases for **Amazon Managed Workflows for Apache Airflow (MWAA)**.
Since Amazon MWAA runs open-source Apache Airflow, most of these examples apply to
self-managed Airflow deployments as well.

## Disclaimer

These are example code samples that demonstrate practical implementations for specific
use cases and scenarios. They are educational examples, not supported products. Any
application you integrate these examples into should be thoroughly tested, secured, and
optimized according to your own security standards and policies before deploying to
production or handling production workloads.

## Use Cases

Each use case contains its own `README.md` with setup and run instructions.

| Use Case | Description |
|---|---|
| [Parent/Child DAG Tracking](usecases/parent-child-dag-tracking/) | Two approaches (Airflow **Variables** and **XCom**) for triggering child DAGs from a parent DAG and reliably tracking whether each child executed. Includes fixes for the common "parent runs but children stay queued" problem. |

## Setup

These examples assume a working Amazon MWAA environment. Tested on Apache Airflow 2.10.3
(`mw1.medium`). Upload the relevant DAG files to the root of your MWAA S3 DAGs bucket —
see the [Amazon MWAA documentation](https://docs.aws.amazon.com/mwaa/) for details.

## Contributing

Contributions of additional use cases are welcome — please submit a Pull Request.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
