import type { ButtonHTMLAttributes } from 'react'

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement>

export function Button({ children, type = 'button', ...props }: ButtonProps): JSX.Element {
  return (
    <button className="button" type={type} {...props}>
      {children}
    </button>
  )
}
