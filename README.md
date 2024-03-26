# gigasheet-python

## Installation

The Gigasheet API wrapper is built for Python 3 and may not work with earlier versions of Python.

### Recommended installation

To run the examples, clone this repository and then run the following steps from the root of the cloned repository:

1. Create a venv (optional, but recommended)

```sh
python -m venv venv
source ./venv/bin/activate
```

2. Install Gigasheet

 - To install from PyPI, you can simply run: `pip install gigasheet`.

3. Run an example. See the [Authentication](#authentication) section for setting your key. After that, try running some examples in the example folder.

```sh
python examples/check_setup.py
```

### Alternate installation as standalone

Because the Gigasheet API wrapper is currently only a single file, you can also download `gigasheet.py` from the `gigasheet` folder of this repository and place that file in the same directory as your own script. You can then import gigasheet with the line `import gigasheet`. However, this is not recommended as it is more brittle. It is preferred to use the setup steps described above instead.

## Authentication

Set the environment variable `GIGASHEET_API_KEY` to be your Gigasheet API key to authenticate requests.

Contact support@gigasheet.com for more information about how to obtain a Gigasheet API key.

## Examples

### Import file from S3 and share with a colleague

You can use the example command-line tool `examples/import_and_share_cli.py` to import a file from S3 and share it to a colleague.

First, use the AWS console or an AWS SDK to obtain a pre-signed link for the file you want to import. Then, use the command-line tool like this:

`python examples/upload_and_share_cli.py --input-url <the s3 presigned link> --share-to <email of colleague> --share-write`

Run `python examples/upload_and_share_cli.py --help` to see more options like setting a file name or a share message.

Alternatively, if you already have AWS credentials on your machine, you could look at the simplified example script:

`python examples/upload_from_s3.py`

### Export from Gigasheet to a local file

You can use this example script to get an S3 presigned URL of an export and to save it to your local disk:

`python examples/download_export.py`

## API endpoint documentation

[https://gigasheet.readme.io/](https://gigasheet.readme.io/)

## License

This project is licensed under the terms of the [MIT license](LICENSE).

## Contact

Visit us at https://gigasheet.com for more about our product.

Contact support@gigasheet.com for more information or for help getting an API key.
