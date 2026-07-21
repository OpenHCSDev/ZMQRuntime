Installation
============

Install the core package from PyPI:

.. code-block:: bash

    pip install zmqruntime

If you're developing locally:

.. code-block:: bash

    git clone https://github.com/OpenHCSDev/zmqruntime.git
    cd zmqruntime
    python -m pip install -e ".[dev,docs]"

Build the documentation with a clean environment and treat warnings as
errors:

.. code-block:: bash

    python -m sphinx -E -W --keep-going -b html docs/source docs/_build/html

The generated site starts at ``docs/_build/html/index.html``.
