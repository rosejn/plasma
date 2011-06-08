(ns example.brain
  (:use plasma.core
        test-utils))

; NOTES:
; * all weight in grams

(def b-graph (open-graph "db/brain.graph"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Structure
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
;The central nervous system (CNS) consists of the brain and the spinal cord ,
;immersed in the cerebrospinal fluid (CSF).
(def CNS {:label :cns
          :description "Central nervous system"})

;Weighing about 3 pounds (1.4 kilograms), the brain consists of three main
;structures: the cerebrum , the cerebellum  and the brainstem.
(def BRAIN {:label :brain
            :description "Brain"
            :weight 1400})

(def SPINAL-CHORD {:label :spinal-chord
                   :description "Spinal chord"})

(def CEREBROSPINAL-FLUID {:label :cerebrospinal-fluid
                          :description "Cerebrospinal fluid"})

;Cerebrum - divided into two hemispheres (left and right), each consists of
;four lobes (frontal, parietal , occipital  and temporal). The outer layer of
;the brain is known as the cerebral cortex    or the ‘grey matter’. It covers
;the nuclei deep within the cerebral hemisphere known as the ‘white matter’.
(def CEREBRUM {:label :cerebrum
               :description ""})

;Grey matter – closely packed neuron  cell bodies form the grey matter  of the
;brain. The grey matter includes regions of the brain involved in muscle
;control, sensory perceptions, such as seeing and hearing, memory, emotions and
;speech.
(def CEREBRAL-CORTEX {:label :cerebral-cortex
                      :description ""})

;Cerebellum – responsible for psychomotor function, the cerebellum co-ordinates
;sensory input from the inner ear and the muscles to provide accurate control
;of position and movement.
(def CEREBELLUM {:label :cerebellum
                 :description ""})

;Brainstem – found at the base of the brain, it forms the link between the
;cerebral cortex, white matter and the spinal cord. The brainstem contributes
;to the control of breathing, sleep and circulation.
(def BRAIN-STEM {:label :brain-stem
                 :description ""})

(def LEFT-HEMISPHERE {:label :left-hemisphere
                      :description ""})

(def RIGHT-HEMISPHERE {:label :right-hemisphere
                       :description ""})

(def FRONTAL-LOBE {:label :frontal-lobe
                   :description ""})

(def PARIETAL-LOBE {:label :parietal-lobe
                   :description ""})

(def OCCIPITAL-LOBE {:label :occipital-lobe
                     :description ""})

(def TEMPORAL-LOBE {:label :temporal-lobe
                   :description ""})


;White matter – neuronal tissue containing mainly long, myelinated axons , is known as white matter  or the diencephalon. Situated between the brainstem and cerebellum, the white matter consists of structures at the core of the brain such as the thalamus  and hypothalamus . The nuclei of the white matter are involved in the relay of sensory information from the rest of the body to the cerebral cortex, as well as in the regulation of autonomic (unconscious) functions such as body temperature, heart rate and blood pressure. Certain nuclei within the white matter are involved in the expression of emotions, the release of hormones from the pituitary gland, and in the regulation of food and water intake. These nuclei are generally considered part of the limbic system .

;The thalamus and hypothalamus are prominent internal structures.

;The thalamus has wide-ranging connections with the cortex and many other parts
;of the brain, such as the basal ganglia, hypothalmus and brainstem. It is
;capable of perceiving pain but not at accurately locating it.
(def THALAMUS {:label :thalamus
               :description ""})

;The hypothalamus has several important functions, including control of the
;body’s appetite, sleep patterns, sexual drive and response to anxiety.
(def HYPOTHALAMUS {:label :hypothalamus
                   :description ""})

;Basal Ganglia
;
;Collectively the caudate nucleus, putamen and globus pallidus form the basal
;ganglia, and are involved in movement control. These highly specialised
;clusters of cells/nuclei are found within the white matter, beneath the
;cerebral cortex.

(def BASAL-GANGLIA {:label :basal-ganglia
                    :description ""})

(def CAUDATE-NUCLEUS {:label :caudate-nucleus
                    :description ""})

(def PUTAMEN {:label :putamen
              :description ""})

(def GLOBUS-PALLIDUS {:label :globus-pallidus
                    :description ""})

;Ventricles
;
;Within the brain there are a number of cavities called ventricles. Ventricles
;are filled with CSF, which is produced within the ventricle wall. The CSF also
;surrounds the outer surfaces of the brain and ‘cushions’ the brain against
;trauma, maintains and control the extracellular environment, and circulates
;endocrine hormones. It is the CSF that is removed from the spine when a lumbar
;puncture (LP) is performed on a patient. Results of an LP can show whether the
;CSF has normal glucose and electrolyte concentrations and whether there is an
;infection in or around the brain.

(def VENTRICLES {:label :ventricles
                 :description ""})

;Limbic System
;
;The limbic system is not a structure, but a series of nerve pathways
;incorporating structures deep within the temporal lobes, such as the
;hippocampus  and the amygdale. Forming connections with the cerebral cortex,
;white matter and brainstem, the limbic system is involved in the control and
;expression of mood and emotion, in the processing and storage of recent
;memory, and in the control of appetite and emotional responses to food. All
;these functions are frequently affected in depression and the limbic system
;has been implicated in the pathogenesis of depression. The limbic system is
;also linked with parts of the neuroendocrine and autonomic nervous systems,
;and some neurological disorders, such as anxiety, are associated with both
;hormonal and autonomic changes.

;Reticular Activating System
;
;At the core of the brainstem is a collection of nuclei called the reticular
;formation.These nuclei receive input from most of the body’s sensory systems
;(eg sight, smell, taste, etc) and other parts of the brain, such as the
;cerebellum and cerebral hemispheres.

;Some neurons from the reticular formation project to meet motor neurons of the
;spinal cord and influence functions such as cardiovascular and respiratory
;control. In addition, there are also neurons projecting into most of the rest
;of the brain. The ascending fibres of the reticular formation form a network
;called the reticular activating system, which influence wakefulness, overall
;degree of arousal and consciousness – all factors which may be disturbed in
;depressed patients.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Cells
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;Although extremely complex, the brain is largely made up of only two principal
;cell types: neurons and glial   cells. There are over 100,000 million neurons
;in the brain and an even greater number of glial cells . It is estimated that
;there are more than 10,000 million cells in the cerebral cortex alone.

;Neurons are involved in information transmission – receiving, processing and
;transmitting information through their highly specialised structure. Neurons
;consist of a cell body and two types of projections – the dendrites  and an
;axon. Most neurons have many dendrites, but only one axon.

;The majority of neurons are unable to undergo cell division or repair. This
;limitation results in irreversible damage to the nervous system after trauma,
;intoxication, oxygen deficiency or stroke.

;Neurons use their highly specialised structure to both send and receive
;signals. Individual neurons receive information from thousands of other
;neurons, and in turn send information to thousands more. Information is passed
;from one neuron to another via neurotransmission. This is an indirect process
;that takes place in the area between the nerve ending (nerve terminal) and the
;next cell body. This area is called the synaptic cleft  or synapse.
(def NEURON {:label :neuron
             :description ""})


;Glia
;
;Glial cells are major constituents of the central nervous system, and while
;they do not have a direct role in neurotransmission, glial cells play a
;supporting role that helps define synaptic contacts and maintain the
;signalling abilities of neurons. Various types of glial cells can be found in
;the brain (or CNS); including astrocytes, oligodendroglia and microglia. The
;total number of glial cells exceeds that of neurons by approximately
;three-fold.

;Glial cells are smaller than neurons and lack axons and dendrites. The
;well-defined roles of the glia include: modulating the rate of nerve impulse
;propagation; controlling the uptake of neurotransmitters; and playing a
;pivotal role during development and adulthood. Some evidence also suggests
;that glial cells aid (or, in some cases, prevent) recovery from neuronal
;injury and that they are involved in a number of diseases, such as Alzheimer’s
;disease, multiple sclerosis  and other central and peripheral neuropathies.
(def GLIA {:label :glia
           :description ""})

(def ASTROCYTES {:label :astrocytes
                 :description ""})

(def MICROGLIA {:label :microglia
                :description ""})

(def OLIGODENDROGLIA {:label :oligodendroglia
                      :description ""})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Function
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Senses
(def TOUCH {})
(def VISION {})
(def HEARING {})
(def TASTE {})
(def SMELL {})

(def MOTOR-CONTROL {})
(def MEMORY {})
(def EMOTION {})

(def MOTIVATION {})
(def ATTENTION {})

(defn brain-graph
  []
  (-> (nodes [root ROOT-ID
              brain BRAIN
              spinal-chord SPINAL-CHORD
              c-fluid      CEREBROSPINAL-FLUID
              cortex          :cortex
              frontal-lobe    :frontal-lobe
              parietal-lobe   :parietal-lobe
              temporal-lobe   :temporal-lobe
              occipital-lobe  :occipital-lobe
              cerebellum      :cerebellum
              ])
    (edges
      [root brain        :contains
       root spinal-chord :contains
       root c-fluid      :contains
       brain cerebrum   :contains
       brain cerebellum :contains
       brain brain-stem :contains
       ])))

