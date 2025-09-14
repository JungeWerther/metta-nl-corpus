from typing import Callable

type Fn[*T, U] = Callable[[*T], U]
type Transformation[T] = Fn[T, T]
