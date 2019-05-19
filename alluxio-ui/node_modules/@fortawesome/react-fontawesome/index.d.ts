/// <reference types="react" />
import { CSSProperties } from 'react'
import {
  Transform,
  IconProp,
  FlipProp,
  SizeProp,
  PullProp,
  RotateProp,
  FaSymbol
} from '@fortawesome/fontawesome-svg-core'

export function FontAwesomeIcon(props: Props): JSX.Element

export interface Props {
  icon: IconProp
  mask?: IconProp
  className?: string
  color?: string
  spin?: boolean
  pulse?: boolean
  border?: boolean
  fixedWidth?: boolean
  inverse?: boolean
  listItem?: boolean
  flip?: FlipProp
  size?: SizeProp
  pull?: PullProp
  rotation?: RotateProp
  transform?: string | Transform
  symbol?: FaSymbol
  style?: CSSProperties
  tabIndex?: number;
  title?: string;
}
