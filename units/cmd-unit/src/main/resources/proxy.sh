#!/bin/bash
{ echo $@ ; cat - ; } | nc %s %d
