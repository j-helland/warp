# Developer Guide

---

## Design Goals

!!!warning
    WARP is still in early development, which means that there is a limited set of functionality and, moreover, the design principles for the tool are not fully realized.
    The following exposition is more idealistic than what is reflected by the current functionality of the tool so as to reflect what the tool *should become*.

WARP itself intends to be a tool, not a framework.
In this sense, the goal of this page is not to give you a specific ideological approach to organizing your pipelines -- rather, the goal is to demonstrate WARP functionality so that you can make your own organizational decisions.

That being said, there are still general practices that WARP intends to encourage through the following design philosophy: complicated / bug-prone pipeline behavior should require explicit code expressions that reflect this complexity.
You can very much do the "wrong thing" in WARP, but the tool should encourage you to be transparent about it.

---
