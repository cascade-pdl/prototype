"""The Docker CLI as a library: commands, images, containers.

Knows nothing about cascade — no plans, stores, refs or run specs — so it sits below every
other package and anything may use it. What cascade *does* with Docker stays with the
concept it serves: building a ``docker run`` line from a run spec in
``cascade.runners.docker``, preparing a run's images in ``cascade.runners.images``, image
names in ``cascade.substrate.naming``.
"""
