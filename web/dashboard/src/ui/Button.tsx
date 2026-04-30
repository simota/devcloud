import type { ButtonHTMLAttributes } from 'react'

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement>

export function Button({ children, className, type = 'button', ...props }: ButtonProps): JSX.Element {
  return (
    <button className={className ? `button ${className}` : 'button'} type={type} {...props}>
      {children}
    </button>
  )
}
