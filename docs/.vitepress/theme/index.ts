import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'

import './vars.css'
import './overrides.css'
import 'uno.css'

const config: Theme = {
  extends: DefaultTheme,
}

export default config
