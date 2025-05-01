import React, { useEffect } from "react";
import styles from "./ModalBase.module.scss";
import { IoClose } from "react-icons/io5";

type ModalBaseProps = {
  closeOnBgClick?: boolean;
  onClose: () => void;
  containerClassName?: string;
  children: React.ReactNode;
  title: string;
  isOpen?: boolean;
};
export default function ModalBase(props: ModalBaseProps) {
  const handleBgClick = () => {
    if (props.closeOnBgClick) props.onClose();
  };

  useEffect(() => {
    if (props.isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "scroll";
    }
  }, [props.isOpen]);
  if (!props.isOpen) return null;
  return (
    <div className={styles["modal-bg"]} onClick={handleBgClick}>
      <div className={styles["content"]} onClick={(e) => e.stopPropagation()}>
        <div className={styles["content__header"]}>
          <span>{props.title}</span>
          <button onClick={props.onClose}>
            <IoClose size={20} />
          </button>
        </div>
        <div
          className={props.containerClassName ? props.containerClassName : ""}
        >
          {props.children}
        </div>
        {}
      </div>
    </div>
  );
}
